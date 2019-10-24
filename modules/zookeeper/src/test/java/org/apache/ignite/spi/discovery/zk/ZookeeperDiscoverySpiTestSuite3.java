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

package org.apache.ignite.spi.discovery.zk;

import org.apache.ignite.internal.processors.continuous.GridEventConsumeSelfTest;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Regular Ignite tests executed with {@link ZookeeperDiscoverySpi}.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//    GridCacheReplicatedNodeRestartSelfTest.class,
    GridEventConsumeSelfTest.class,
//    GridCachePartitionedNodeRestartTxSelfTest.class,
//    IgniteClientDataStructuresTest.class,
//    GridCacheReplicatedSequenceApiSelfTest.class,
//    GridCachePartitionedSequenceApiSelfTest.class,
//    GridCacheAtomicMultiJvmFullApiSelfTest.class,
//    GridCachePartitionedMultiJvmFullApiSelfTest.class,
//    GridP2PContinuousDeploymentSelfTest.class,
//    CacheContinuousQueryOperationP2PTest.class,
//    CacheContinuousQueryLongP2PTest.class
})
public class ZookeeperDiscoverySpiTestSuite3 {
    /** */
    @BeforeClass
    public static void init() throws Exception {
        ZookeeperDiscoverySpiTestConfigurator.initTestSuite();
    }
}
