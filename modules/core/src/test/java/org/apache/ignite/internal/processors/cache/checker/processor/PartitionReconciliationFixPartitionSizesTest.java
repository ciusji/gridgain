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

package org.apache.ignite.internal.processors.cache.checker.processor;

import com.sun.tools.javac.util.List;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationResult;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.visor.checker.VisorPartitionReconciliationTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.lang.Thread.sleep;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.FINISHED;
import static org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationEventListener.WorkLoadStage.READY;

/**
 * Tests count of calls the recheck process with different inputs.
 */
public class PartitionReconciliationFixPartitionSizesTest extends PartitionReconciliationAbstractTest {
    /** Nodes. */
    protected static final int NODES_CNT = 1;

    /** Crd server node. */
    protected IgniteEx ig;

    /** Client. */
    protected IgniteEx client;

    private Random rnd = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(DEFAULT_CACHE_NAME);
//        ccfg.setGroupName("zzz");
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1));
        ccfg.setBackups(NODES_CNT - NODES_CNT);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);
        cfg.setConsistentId(name);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        ig = startGrids(NODES_CNT);

        client = startClientGrid(NODES_CNT);

        ig.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void testRepair() throws Exception {
        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++) {
            cache.put(i, i);
        }

        setPartitionSize(grid(0), DEFAULT_CACHE_NAME, 0, 58);
        setPartitionSize(grid(0), DEFAULT_CACHE_NAME, 1, -129);
        setPartitionSize(grid(1), DEFAULT_CACHE_NAME, 0, 536);
        setPartitionSize(grid(1), DEFAULT_CACHE_NAME, 1, 139);

        assertFalse(cache.size() == 100);

//        doSleep(500);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(1);
//        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME, "qqq"));
        Set<String> objects = new HashSet<>();
        objects.add(DEFAULT_CACHE_NAME);
//        objects.add("qqq");
        builder.caches(objects);

        AtomicReference<ReconciliationResult> res = new AtomicReference<>();

        GridTestUtils.runMultiThreadedAsync(() -> res.set(partitionReconciliation(client, builder)), 1, "reconciliation");

        GridTestUtils.waitForCondition(() -> res.get() != null, 40_000);

        ReconciliationResult reconciliationRes = res.get();


        assertEquals(100, cache.size());
//        assertEquals(0, res.get().partitionReconciliationResult().inconsistentKeysCount());
//        org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationResultCollector.Simple.partSizesMap
//        internalCache(grid(0).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(0)
//        PartitionReconciliationProcessor#execute
//        CollectPartitionKeysByBatchTask.CollectPartitionKeysByBatchJob.execute0
    }

    @Test
    public void testRepairUnderLoad() throws Exception {
        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        int startKey = 0;
        int endKey = 10;

        for (int i = startKey; i < endKey; i++) {
            i += 2;
            if (i < endKey)
                cache.put(i, i);
        }

        int startSize = cache.size();

//        setPartitionSize(grid(0), DEFAULT_CACHE_NAME, 0, 58);
//        setPartitionSize(grid(0), DEFAULT_CACHE_NAME, 1, -129);
//        setPartitionSize(grid(1), DEFAULT_CACHE_NAME, 0, 536);
//        setPartitionSize(grid(1), DEFAULT_CACHE_NAME, 1, 139);

//        breakCacheSizes(List.of(grid(0)), List.of(DEFAULT_CACHE_NAME));
//
//        assertFalse(cache.size() == startSize);

        doSleep(500);

        VisorPartitionReconciliationTaskArg.Builder builder = new VisorPartitionReconciliationTaskArg.Builder();
        builder.repair(true);
        builder.parallelism(1);
//        builder.caches(Collections.singleton(DEFAULT_CACHE_NAME, "qqq"));
        Set<String> objects = new HashSet<>();
        objects.add(DEFAULT_CACHE_NAME);
//        objects.add("qqq");
        builder.caches(objects);


        AtomicReference<ReconciliationResult> res = new AtomicReference<>();

        IgniteInternalFuture loadFut = GridTestUtils.runAsync(() -> {
            System.out.println("qvsdhntsd loadFut start");

            int i = 0;

            int max = 0;

            while(res.get() == null/* || i < endKey*/) {
                int i1 = startKey + rnd.nextInt(endKey - startKey);
                cache.put(i1, 1);

//                System.out.println("qfegsdg put random: " + i1);
//                doSleep(3);

                if (i1 > max)
                    max = i1;

//                if (i < endKey) {
//                    cache.put(i, i);
//                    i++;
//                }
            }

            System.out.println("qvraslpf loadFut stop" + i);
            System.out.println("qmfgtssf loadFut max" + max);
        });

        System.out.println("qvsdhntsd partitionReconciliation start");

        GridTestUtils.runMultiThreadedAsync(() -> res.set(partitionReconciliation(client, builder)), 1, "reconciliation");

        GridTestUtils.waitForCondition(() -> res.get() != null, 40_000);

        System.out.println("qvsdhntsd partitionReconciliation stop");

        ReconciliationResult reconciliationRes = res.get();

        loadFut.get();

//        doSleep(5000);

//        endKey = 1000;

        for (int i = startKey; i < endKey; i++) {
            cache.put(i, i);

//            System.out.println("qfegsdg put after all: " + i);
        }
        assertEquals(endKey, grid(0).cache(DEFAULT_CACHE_NAME).size());
//        assertEquals(0, res.get().partitionReconciliationResult().inconsistentKeysCount());
//        org.apache.ignite.internal.processors.cache.checker.processor.ReconciliationResultCollector.Simple.partSizesMap
//        internalCache(grid(0).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(0)
//        PartitionReconciliationProcessor#execute
//        CollectPartitionKeysByBatchTask.CollectPartitionKeysByBatchJob.execute0

//        int cacheId = client.context().cache().cache(DEFAULT_CACHE_NAME).context().cacheId();
//        long delta00 = ((internalCache(grid(0).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(0).dataStore())).reconciliationCtx().storageSizeDelta(cacheId);
    }

    private void setPartitionSize(IgniteEx grid, String cacheName, int partId, int delta) {

        GridCacheContext<Object, Object> cctx = grid.context().cache().cache(cacheName).context();

        int cacheId = cctx.cacheId();

        cctx.group().topology().localPartition(partId).dataStore().updateSize(cacheId, delta);
    }

    private void setPartitionSize(IgniteEx grid, String cacheName) {

        GridCacheContext<Object, Object> cctx = grid.context().cache().cache(cacheName).context();

        int cacheId = cctx.cacheId();

        cctx.group().topology().localPartitions().forEach(part -> part.dataStore().updateSize(cacheId, rnd.nextInt()));

//        cctx.group().topology().localPartition(partId).dataStore().updateSize(cacheId, delta);
    }

    private void breakCacheSizes(List<IgniteEx> nodes, List<String> cacheNames) {
        nodes.forEach(node -> {
            cacheNames.forEach(cacheName -> {
                setPartitionSize(node, cacheName);
            });
        });
    }
}
