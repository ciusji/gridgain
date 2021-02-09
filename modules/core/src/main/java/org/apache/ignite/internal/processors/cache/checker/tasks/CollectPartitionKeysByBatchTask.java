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

package org.apache.ignite.internal.processors.cache.checker.tasks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.ExecutionResult;
import org.apache.ignite.internal.processors.cache.checker.objects.PartitionBatchRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedKey;
import org.apache.ignite.internal.processors.cache.checker.util.KeyComparator;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.lang.Thread.sleep;
import static org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.DATA;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.unmarshalKey;

/**
 * Collects and returns a set of keys that have conflicts with {@link GridCacheVersion}.
 */
@GridInternal
public class CollectPartitionKeysByBatchTask extends ComputeTaskAdapter<PartitionBatchRequest, ExecutionResult<T3<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>, Map<UUID, Long>>>> {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    private static final KeyComparator KEY_COMPARATOR = new KeyComparator();

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** Partition batch. */
    private volatile PartitionBatchRequest partBatch;

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        PartitionBatchRequest partBatch) throws IgniteException {
        Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

        this.partBatch = partBatch;

        for (ClusterNode node : subgrid)
            jobs.put(new CollectPartitionKeysByBatchJob(partBatch), node);

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        ComputeJobResultPolicy superRes = super.result(res, rcvd);

        // Deny failover.
        if (superRes == ComputeJobResultPolicy.FAILOVER) {
            superRes = ComputeJobResultPolicy.WAIT;

            log.warning("CollectPartitionEntryHashesJob failed on node " +
                "[consistentId=" + res.getNode().consistentId() + "]", res.getException());
        }

        return superRes;
    }

    /** {@inheritDoc} */
    @Override public @Nullable ExecutionResult<T3<KeyCacheObject, Map<KeyCacheObject, Map<UUID, GridCacheVersion>>, Map<UUID, Long>>> reduce(
        List<ComputeJobResult> results) throws IgniteException {
        assert partBatch != null;

        GridCacheContext<Object, Object> ctx = ignite.context().cache().cache(partBatch.cacheName()).context();

        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> totalRes = new HashMap<>();

        KeyCacheObject lastKey = null;

        Map<UUID, Long> partSizesMap = new HashMap<>();

        for (int i = 0; i < results.size(); i++) {
            UUID nodeId = results.get(i).getNode().id();

            IgniteException exc = results.get(i).getException();

            if (exc != null)
                return new ExecutionResult<>(exc.getMessage());

            ExecutionResult<T2<List<VersionedKey>, Long>> nodeRes = results.get(i).getData();

            if (nodeRes.errorMessage() != null)
                return new ExecutionResult<>(nodeRes.errorMessage());

            for (VersionedKey partKeyVer : nodeRes.result().get1()) {
                try {
                    KeyCacheObject key = unmarshalKey(partKeyVer.key(), ctx);

                    if (lastKey == null || KEY_COMPARATOR.compare(lastKey, key) < 0)
                        lastKey = key;

                    Map<UUID, GridCacheVersion> map = totalRes.computeIfAbsent(key, k -> new HashMap<>());
                    map.put(partKeyVer.nodeId(), partKeyVer.ver());

                    if (i == (results.size() - 1) && map.size() == results.size() && !hasConflict(map.values()))
                        totalRes.remove(key);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, e.getMessage(), e);

                    return new ExecutionResult<>(e.getMessage());
                }
            }

            partSizesMap.put(nodeId, nodeRes.result().get2());
        }

//         if (lastKey == null)
//             System.out.println("qgrtsngd null");
//         else
//             System.out.println("qgrtsngd " + ((KeyCacheObjectImpl)lastKey).value());

        return new ExecutionResult<>(new T3<>(lastKey, totalRes, partSizesMap));
    }

    /**
     *
     */
    private boolean hasConflict(Collection<GridCacheVersion> keyVersions) {
        assert !keyVersions.isEmpty();

        Iterator<GridCacheVersion> iter = keyVersions.iterator();
        GridCacheVersion ver = iter.next();

        while (iter.hasNext()) {
            if (!ver.equals(iter.next()))
                return true;
        }

        return false;
    }

    /**
     *
     */
    public static class CollectPartitionKeysByBatchJob extends ReconciliationResourceLimitedJob {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /** Partition key. */
        private PartitionBatchRequest partBatch;

        /**
         * @param partBatch Partition key.
         */
        private CollectPartitionKeysByBatchJob(PartitionBatchRequest partBatch) {
            this.partBatch = partBatch;
        }

        /** {@inheritDoc} */
        @Override protected long sessionId() {
            return partBatch.sessionId();
        }

        /** {@inheritDoc} */
        @Override protected ExecutionResult<T2<List<VersionedKey>, Long>> execute0() {
            GridCacheContext<Object, Object> cctx = ignite.context().cache().cache(partBatch.cacheName()).context();

            CacheGroupContext grpCtx = cctx.group();

            int cacheId = cctx.cacheId();

            final int batchSize = partBatch.batchSize();
            final KeyCacheObject lowerKey;

            try {
                lowerKey = unmarshalKey(partBatch.lowerKey(), cctx);
            }
            catch (IgniteCheckedException e) {
                String errMsg = "Batch [" + partBatch + "] can't processed. Broken key.";

                log.error(errMsg, e);

                return new ExecutionResult<>(errMsg + " " + e.getMessage());
            }

            GridDhtLocalPartition part = grpCtx.topology().localPartition(partBatch.partitionId());

            IgniteCacheOffheapManager.CacheDataStore cacheDataStore = grpCtx.offheap().dataStore(part);

            assert part != null;

            part.reserve();

            IgniteCacheOffheapManagerImpl.CacheDataStoreImpl.ReconciliationContext partReconciliationCtx = cacheDataStore.reconciliationCtx();

            if (lowerKey == null)
                partReconciliationCtx.isReconciliationInProgress(true);

            KeyCacheObject lastKeyForSizes = partReconciliationCtx.lastKey(cacheId);

            KeyCacheObject keyToStart = null;

            if (lowerKey != null && lastKeyForSizes !=  null)
                keyToStart = KEY_COMPARATOR.compare(lowerKey, lastKeyForSizes) < 0 ? lowerKey : lastKeyForSizes;
            else if (lowerKey != null)
                keyToStart = lowerKey;
            else if (lowerKey != null)
                keyToStart = lastKeyForSizes;


            try (GridCursor<? extends CacheDataRow> cursor = keyToStart == null ?
                grpCtx.offheap().dataStore(part).cursor(cacheId, DATA) :
                grpCtx.offheap().dataStore(part).cursor(cacheId, keyToStart, null)) {

                List<VersionedKey> partEntryHashRecords = new ArrayList<>();

                Long partSize = partBatch.partSizesMap().get(ignite.localNode().id());

                if (partSize == null)
                    partSize = 0L;

                for (int i = 0; i < batchSize && cursor.next(); i++) {
//                    System.out.println("qfvndrfg");

                    CacheDataRow row;

                    try {
                        sleep(10);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    synchronized (partReconciliationCtx.reconciliationMux()) {
                        row = cursor.get();

                        if (partReconciliationCtx.lastKey(cacheId) == null || KEY_COMPARATOR.compare(partReconciliationCtx.lastKey(cacheId), row.key()) < 0) {
                            partReconciliationCtx.lastKey(cacheId, row.key());
//                            System.out.println("qqedfks1 " + ignite.localNode().id() +
//                                " reconcilation execute0 if. _cacheDataStore.lastKey()_: " + (cacheDataStore.lastKey() == null ? "null" : cacheDataStore.lastKey()) +
//                                " ||| _row.key()_:" + row.key() +
//                                " ||| compare: " + (cacheDataStore.lastKey() == null ? "null" : KEY_COMPARATOR.compare(cacheDataStore.lastKey(), row.key())) +
//                                " ||| partId: " + part +
//                                " ||| partSize: " + partSize);
//                            System.out.println("qqedfks2 " + ignite.localNode().id() +
//                                " reconcilation execute0 if. _cacheDataStore.lastKey()_: " + (((KeyCacheObjectImpl)cacheDataStore.lastKey()).value() == null ? "null" : ((KeyCacheObjectImpl)cacheDataStore.lastKey()).value()) +
//                                " ||| _row.key()_:" + ((KeyCacheObjectImpl) row.key()).value() +
//                                " ||| compare: " + (((KeyCacheObjectImpl) cacheDataStore.lastKey()).value() == null ? "null" : ((Integer)((KeyCacheObjectImpl) cacheDataStore.lastKey()).value()) > ((Integer)((KeyCacheObjectImpl) row.key()).value())) +
//                                " ||| partId: " + part +
//                                " ||| partSize: " + partSize);
                        }
                        else {
//                            System.out.println("qftsbg1 " + ignite.localNode().id() +
//                                " reconcilation execute0 else. _cacheDataStore.lastKey()_: " + (cacheDataStore.lastKey() == null ? "null" : cacheDataStore.lastKey()) +
//                                " ||| _row.key()_:" + cacheDataStore.lastKey() +
//                                " ||| compare: " + (cacheDataStore.lastKey() == null ? "null" : KEY_COMPARATOR.compare(cacheDataStore.lastKey(), row.key())) +
//                                " ||| partId: " + part +
//                                " ||| partSize: " + partSize);
//                            System.out.println("qftsbg2 " + ignite.localNode().id() +
//                                " reconcilation execute0 else. _cacheDataStore.lastKey()_: " + (((KeyCacheObjectImpl)cacheDataStore.lastKey()).value() == null ? "null" : ((KeyCacheObjectImpl)cacheDataStore.lastKey()).value()) +
//                                " ||| _row.key()_:" + ((KeyCacheObjectImpl)row.key()).value() +
//                                " ||| compare: " + (((KeyCacheObjectImpl)cacheDataStore.lastKey()).value() == null ? "null" : ((Integer)((KeyCacheObjectImpl)cacheDataStore.lastKey()).value()) > ((Integer)((KeyCacheObjectImpl)row.key()).value())) +
//                                " ||| partId: " + part +
//                                " ||| partSize: " + partSize);
                        }
                    }
                    System.out.println("qdvrfgad " + ignite.localNode().id() + " _row.key()_:" + ((KeyCacheObjectImpl)row.key()).value() + ", lowerKey: " + (lowerKey == null ? "null" : ((KeyCacheObjectImpl)lowerKey).value()) + ", row.key(): " + row.key() + ", lowerKey: " + lowerKey);
                    if (lowerKey == null || KEY_COMPARATOR.compare(lowerKey, row.key()) != 0) {
//                    if (lowerKey == null || !((KeyCacheObjectImpl)row.key()).value().equals(((KeyCacheObjectImpl)lowerKey).value())) {
                        partSize++;
//                        System.out.println("qdrvgsrwe partSize: " + partSize);
                        System.out.println("qwerdcs " + ignite.localNode().id() + " _row.key()_:" + ((KeyCacheObjectImpl)row.key()).value());
                        partEntryHashRecords.add(new VersionedKey(
                            ignite.localNode().id(),
                            row.key(),
                            row.version()
                        ));
                    }
                    else
                        i--;
                }

//                System.out.println("qflyruc cursor.next(): " + cursor.next());

                return new ExecutionResult<>(new T2<>(partEntryHashRecords, partSize));
            }
            catch (Exception e) {
                String errMsg = "Batch [" + partBatch + "] can't processed. Broken cursor.";

                log.error(errMsg, e);

                return new ExecutionResult<>(errMsg + " " + e.getMessage());
            }
            finally {
                part.release();
            }
        }
    }
}
