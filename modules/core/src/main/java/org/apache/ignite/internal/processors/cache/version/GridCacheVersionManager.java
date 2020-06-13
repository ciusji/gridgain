/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.version;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;

/**
 * Makes sure that cache lock order values come in proper sequence.
 * <p>
 * NOTE: this class should not make use of any cache specific structures,
 * like, for example GridCacheContext, as it may be reused between different
 * caches.
 */
public class GridCacheVersionManager extends GridCacheSharedManagerAdapter {
    /** */
    public static final GridCacheVersion EVICT_VER = new GridCacheVersion(Integer.MAX_VALUE, 0, 0, 0);

    /** Timestamp used as base time for cache topology version (January 1, 2014). */
    public static final long TOP_VER_BASE_TIME = 1388520000000L;

    /** Last data version metric name. */
    public static final String LAST_DATA_VER = "LastDataVersion";

    /** Last version metric. */
    protected AtomicLongMetric lastDataVer;

    /**
     * Current order. Initialize to current time to make sure that
     * local version increments even after restarts.
     */
    private final AtomicLong order = new AtomicLong(U.currentTimeMillis());

    /** Current order for store operations. */
    private final AtomicLong loadOrder = new AtomicLong(0);

    /** Entry start version. */
    private GridCacheVersion startVer;

    /** Data center ID. */
    private byte dataCenterId;

    /**
     * Oldest cluster node start timestamp, lazily initialized.
     *
     * @see DiscoverySpi#getGridStartTime().
     */
    //private volatile long gridStartTime;

    /**
     * Offset in seconds from {@link #TOP_VER_BASE_TIME}.
     * Added to version to make sure it grows after restart.
     */
    private volatile int offset;

    /** */
    private GridCacheVersion ISOLATED_STREAMER_VER;

    /** */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            assert evt.type() == EVT_NODE_METRICS_UPDATED;

            DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            ClusterNode node = cctx.discovery().node(discoEvt.node().id());

            if (node != null && !node.id().equals(cctx.localNodeId()))
                onReceived(discoEvt.eventNode().id(), node.metrics().getLastDataVersion());
        }
    };

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        MetricRegistry sysreg = cctx.kernalContext().metric().registry(CACHE_METRICS);

        lastDataVer = sysreg.longMetric(LAST_DATA_VER, "The latest data version on the node.");

        lastDataVer.value(order.get());

        startVer = new GridCacheVersion(0, 0, 0, dataCenterId);

        cctx.gridEvents().addLocalEventListener(discoLsnr, EVT_NODE_METRICS_UPDATED);
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        cctx.gridEvents().removeLocalEventListener(discoLsnr, EVT_NODE_METRICS_UPDATED);
    }

    /**
     * @param topVer Topology version.
     */
    public void onLocalJoin(long topVer) {
        long startTime = cctx.kernalContext().discovery().gridStartTime();

        if (startTime != 0)
            offset = (int) ((startTime - TOP_VER_BASE_TIME) / 1000);

        ISOLATED_STREAMER_VER = new GridCacheVersion(1, 0, 1, dataCenterId);
    }

    /**
     * Sets data center ID.
     *
     * @param dataCenterId Data center ID.
     */
    public void dataCenterId(byte dataCenterId) {
        this.dataCenterId = dataCenterId;

        startVer = new GridCacheVersion(0, 0, 0, dataCenterId);

        ISOLATED_STREAMER_VER = new GridCacheVersion(1, 0, 1, dataCenterId);
    }

    /**
     * @param nodeId Node ID.
     * @param ver Remote version.
     */
    public void onReceived(UUID nodeId, GridCacheVersion ver) {
        onReceived(nodeId, ver.order());
    }

    /**
     * @param nodeId Node ID.
     * @param ver Remote version.
     */
    public void onReceived(UUID nodeId, long ver) {
        if (ver > 0) {
            while (true) {
                long order = this.order.get();

                // If another version is larger, we update.
                if (ver > order) {
                    if (!this.order.compareAndSet(order, ver))
                        // Try again.
                        continue;
                    else if (log.isDebugEnabled())
                        log.debug("Updated version from node [nodeId=" + nodeId + ", ver=" + ver + ']');
                }
                else if (log.isDebugEnabled()) {
                    log.debug("Did not update version from node (version has lower order) [nodeId=" + nodeId +
                        ", ver=" + ver + ", curOrder=" + this.order + ']');
                }

                break;
            }
        }
    }

    /**
     * @param rcvOrder Received order.
     */
    public void onExchange(long rcvOrder) {
        long order;

        while (true) {
            order = this.order.get();

            if (rcvOrder > order) {
                if (this.order.compareAndSet(order, rcvOrder))
                    break;
            }
            else
                break;
        }
    }

    /**
     * @param nodeId Node ID.
     * @param ver Received version.
     * @return Next version.
     */
    public GridCacheVersion onReceivedAndNext(UUID nodeId, GridCacheVersion ver) {
        onReceived(nodeId, ver);

        return next(ver);
    }

    /**
     * Version for entries loaded with isolated streamer, should be less than any version generated
     * for entries update.
     *
     * @return Version for entries loaded with isolated streamer.
     */
    public GridCacheVersion isolatedStreamerVersion() {
        return ISOLATED_STREAMER_VER;
    }

    public GridCacheVersion next(AffinityTopologyVersion topVer) {
        return next(topVer.topologyVersion() + offset, topVer.minorTopologyVersion(), dataCenterId);
    }

    public GridCacheVersion next(AffinityTopologyVersion topVer, byte dataCenterId) {
        return next(topVer.topologyVersion() + offset, topVer.minorTopologyVersion(), dataCenterId);
    }

    /**
     * Gets next version based on given topology version. Given value should be
     * real topology version calculated as number of grid topology changes and
     * obtained from discovery manager.
     *
     * @param topVer Topology version for which new version should be obtained.
     * @return Next version based on given topology version.
     */
    public GridCacheVersion next(long topVer) {
        return next(topVer + offset, cctx.localNode().order(), dataCenterId);
    }

    /**
     * Gets next version based on given cache version.
     *
     * @param ver Cache version for which new version should be obtained.
     * @return Next version based on given cache version.
     */
    public GridCacheVersion next(GridCacheVersion ver) {
        return next(ver.topologyVersion(), cctx.localNode().order(), dataCenterId);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @return Next version for cache store operations.
     */
    public GridCacheVersion nextForLoad() {
        return nextForLoad(cctx.kernalContext().discovery().topologyVersion() + offset, cctx.localNode().order(), dataCenterId);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @param topVer Topology version for which new version should be obtained.
     * @return Next version for cache store operations.
     */
    public GridCacheVersion nextForLoad(long topVer) {
        return nextForLoad(topVer + offset, cctx.localNode().order(), dataCenterId);
    }

    /**
     * Gets next version for cache store load and reload operations.
     *
     * @return Next version for cache store operations.
     */
    public GridCacheVersion nextForLoad(GridCacheVersion ver) {
        return nextForLoad(ver.topologyVersion(), cctx.localNode().order(), dataCenterId);
    }

    private GridCacheVersion next(long topVer, long minorTopVer, byte dataCenterId) {
        long ord = order.incrementAndGet();

        GridCacheVersion next = new GridCacheVersion(
            (int)topVer,
            ord,
            (int) minorTopVer,
            dataCenterId);

        lastDataVer.value(ord);

        return next;
    }

    private GridCacheVersion nextForLoad(long topVer, long minorTopVer, byte dataCenterId) {
        long ord = loadOrder.incrementAndGet();

        GridCacheVersion next = new GridCacheVersion(
            (int)topVer,
            ord,
            (int) minorTopVer,
            dataCenterId);

        lastDataVer.value(ord);

        return next;
    }

    /**
     * Gets last generated version without generating a new one.
     * @param topVer Ready topology version.
     *
     * @return Last generated version.
     *
     * @deprecated Kept for compatibility.
     */
    @Deprecated
    public GridCacheVersion last(@Nullable GridDhtPartitionsExchangeFuture fut) {
        return fut == null ? new GridCacheVersion(0, order.get(), 0, dataCenterId) : // For backward compatibility.
            new GridCacheVersion((int)fut.topologyVersion().topologyVersion(), order.get(), (int)cctx.localNode().order(), dataCenterId);
    }

    /**
     * Gets start version.
     *
     * @return Start version.
     */
    public GridCacheVersion startVersion() {
        assert startVer != null;

        return startVer;
    }

    /**
     * Check if given version is start version.
     *
     * @param ver Version.
     * @return {@code True} if given version is start version.
     */
    public boolean isStartVersion(GridCacheVersion ver) {
        return startVer.equals(ver);
    }

    /**
     * Invalidates first cluster node start timestamp, it can be reinitialized lazily in the future.
     */
    public void gridStartTime(long startTime) {
        offset = (int) ((startTime - TOP_VER_BASE_TIME) / 1000);
    }
}
