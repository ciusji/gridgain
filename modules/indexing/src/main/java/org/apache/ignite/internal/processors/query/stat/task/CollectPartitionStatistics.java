/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.stat.task;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.ColumnStatisticsCollector;
import org.apache.ignite.internal.processors.query.stat.ObjectPartitionStatisticsImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.gridgain.internal.h2.table.Column;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Implementation of statistic collector.
 */
public class CollectPartitionStatistics implements Callable<ObjectPartitionStatisticsImpl> {
    /** Canceled check interval. */
    private static final int CANCELLED_CHECK_INTERVAL = 100;

    /** */
    private final GridH2Table tbl;

    /** */
    private final Column[] cols;

    /** */
    private final int partId;

    /** */
    private final long ver;

    /** */
    private final Supplier<Boolean> cancelled;

    /** */
    private final IgniteLogger log;

    /** */
    private final long time = U.currentTimeMillis();

    /** */
    public CollectPartitionStatistics(
        GridH2Table tbl,
        Column[] cols,
        int partId,
        long ver,
        Supplier<Boolean> cancelled,
        IgniteLogger log
    ) {
        this.tbl = tbl;
        this.cols = cols;
        this.partId = partId;
        this.ver = ver;
        this.cancelled = cancelled;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public ObjectPartitionStatisticsImpl call() {
        CacheGroupContext grp = tbl.cacheContext().group();

        GridDhtPartitionTopology top = grp.topology();
        AffinityTopologyVersion topVer = top.readyTopologyVersion();

        GridDhtLocalPartition locPart = top.localPartition(partId, topVer, false);

        if (locPart == null)
            return null;

        boolean reserved = locPart.reserve();

        try {
            if (!reserved || (locPart.state() != OWNING)) {
                log.info("+++ RETRY!!!");
                if (locPart.state() == LOST)
                    return null;

                return null;
            }

            ColumnStatisticsCollector[] collectors = new ColumnStatisticsCollector[cols.length];

            for (int i = 0; i < cols.length; ++i)
                collectors[i] = new ColumnStatisticsCollector(cols[i], tbl::compareValues);

            GridQueryTypeDescriptor typeDesc = tbl.rowDescriptor().type();

            try {
                int checkInt = CANCELLED_CHECK_INTERVAL;

                for (CacheDataRow row : grp.offheap().cachePartitionIterator(
                    tbl.cacheId(), partId, null, false))
                {
                    if (--checkInt == 0) {
                        if (cancelled.get())
                            return null;

                        checkInt = CANCELLED_CHECK_INTERVAL;
                    }

                    if (!typeDesc.matchType(row.value()) || wasExpired(row))
                        continue;

                    H2Row h2row = tbl.rowDescriptor().createRow(row);

                    for (ColumnStatisticsCollector colStat : collectors)
                        colStat.add(h2row.getValue(colStat.col().getColumnId()));
                }
            }
            catch (IgniteCheckedException e) {
                log.warning(String.format("Unable to collect partition level statistics by %s.%s:%d due to %s",
                    tbl.identifier().schema(), tbl.identifier().table(), partId, e.getMessage()));
            }

            Map<String, ColumnStatistics> colStats = Arrays.stream(collectors).collect(
                Collectors.toMap(csc -> csc.col().getName(), ColumnStatisticsCollector::finish));

            return new ObjectPartitionStatisticsImpl(
                partId,
                colStats.values().iterator().next().total(),
                locPart.updateCounter(),
                colStats,
                ver
            );
        }
        finally {
            if (reserved)
                locPart.release();
        }
    }

    /** */
    private boolean wasExpired(CacheDataRow row) {
        return row.expireTime() > 0 && row.expireTime() <= time;
    }
}
