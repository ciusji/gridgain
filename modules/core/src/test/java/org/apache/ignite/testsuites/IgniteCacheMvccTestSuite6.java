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

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.CacheIgniteOutOfMemoryExceptionTest;
import org.apache.ignite.internal.processors.cache.PartitionedAtomicCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.PartitionedMvccTxPessimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.PartitionedTransactionalOptimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.PartitionedTransactionalPessimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.PartitionsExchangeCoordinatorFailoverTest;
import org.apache.ignite.internal.processors.cache.ReplicatedAtomicCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.ReplicatedMvccTxPessimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.ReplicatedTransactionalOptimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.ReplicatedTransactionalPessimisticCacheGetsDistributionTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteExchangeLatchManagerCoordinatorFailTest;
import org.apache.ignite.internal.processors.cache.datastructures.IgniteExchangeLatchManagerDiscoHistoryTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheExchangeMergeTest;
import org.apache.ignite.internal.processors.cache.distributed.CacheParallelStartTest;
import org.apache.ignite.internal.processors.cache.distributed.ExchangeMergeStaleServerNodesTest;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePartitionEvictionDuringReadThroughSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCache150ClientsTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteOptimisticTxSuspendResumeTest;
import org.apache.ignite.internal.processors.cache.distributed.OnePhaseCommitAndNodeLeftTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch.ExchangeLatchManagerTest;
import org.apache.ignite.internal.processors.cache.transactions.TxLocalDhtMixedCacheModesTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOptimisticOnPartitionExchangeTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOptimisticPrepareOnUnstableTopologyTest;
import org.apache.ignite.internal.processors.cache.transactions.TxRollbackOnTimeoutOnePhaseCommitTest;
import org.apache.ignite.internal.processors.cache.transactions.TxStateChangeEventTest;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheMvccTestSuite6 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        System.setProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS, "true");

        Set<Class> ignoredTests = new HashSet<>();

        // Skip classes that already contains Mvcc tests
        ignoredTests.add(TxStateChangeEventTest.class);

        // Atomic cache tests.
        ignoredTests.add(ReplicatedAtomicCacheGetsDistributionTest.class);
        ignoredTests.add(PartitionedAtomicCacheGetsDistributionTest.class);
        ignoredTests.add(GridCachePartitionEvictionDuringReadThroughSelfTest.class);

        // Irrelevant Tx tests.
        ignoredTests.add(IgniteOptimisticTxSuspendResumeTest.class);
        ignoredTests.add(TxOptimisticPrepareOnUnstableTopologyTest.class);
        ignoredTests.add(ReplicatedTransactionalOptimisticCacheGetsDistributionTest.class);
        ignoredTests.add(PartitionedTransactionalOptimisticCacheGetsDistributionTest.class);
        ignoredTests.add(TxOptimisticOnPartitionExchangeTest.class);

        ignoredTests.add(TxRollbackOnTimeoutOnePhaseCommitTest.class);

        // Other non-tx tests.
        ignoredTests.add(CacheExchangeMergeTest.class);
        ignoredTests.add(ExchangeMergeStaleServerNodesTest.class);
        ignoredTests.add(IgniteExchangeLatchManagerCoordinatorFailTest.class);
        ignoredTests.add(IgniteExchangeLatchManagerDiscoHistoryTest.class);
        ignoredTests.add(ExchangeLatchManagerTest.class);
        ignoredTests.add(PartitionsExchangeCoordinatorFailoverTest.class);
        ignoredTests.add(CacheParallelStartTest.class);
        ignoredTests.add(IgniteCache150ClientsTest.class);
        ignoredTests.add(CacheIgniteOutOfMemoryExceptionTest.class);
        ignoredTests.add(OnePhaseCommitAndNodeLeftTest.class);

        // Mixed local/dht tx test.
        ignoredTests.add(TxLocalDhtMixedCacheModesTest.class);

        // Skip tests that has Mvcc clones.
        ignoredTests.add(PartitionedTransactionalPessimisticCacheGetsDistributionTest.class); // See PartitionedMvccTxPessimisticCacheGetsDistributionTest.
        ignoredTests.add(ReplicatedTransactionalPessimisticCacheGetsDistributionTest.class); //See ReplicatedMvccTxPessimisticCacheGetsDistributionTest

        List<Class<?>> suite = new ArrayList<>((IgniteCacheTestSuite6.suite(ignoredTests)));

        // Add mvcc versions for skipped tests.
        suite.add(PartitionedMvccTxPessimisticCacheGetsDistributionTest.class);
        suite.add(ReplicatedMvccTxPessimisticCacheGetsDistributionTest.class);

        return suite;
    }
}
