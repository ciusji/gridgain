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

package org.apache.ignite.spi.discovery;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Test check that disco-event-worker proccessed ChangeGlobalStateMessage before
 * disco-notifier-worker start processing of ChangeGlobalStateFinishMessage
 */
public class ChangeGlobalStateMessageOrderTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testChangeGlobalStateMessageOrder() throws Exception {
        startGrid(0);

        IgniteEx client = startClientGrid(1);

        DiscoveryEventListener testEvtLsnr = new TestEventListener(client);

        client.context().event().addDiscoveryEventListener(testEvtLsnr, EVT_DISCOVERY_CUSTOM_EVT);

        client.cluster().state(ClusterState.ACTIVE);

        assertTrue(client.cluster().state() == ClusterState.ACTIVE);

        client.cluster().state(ClusterState.INACTIVE);

        assertTrue(client.cluster().state() == ClusterState.INACTIVE);

        client.cluster().state(ClusterState.ACTIVE);

        assertTrue(client.cluster().state() == ClusterState.ACTIVE);
    }

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    private static class TestEventListener implements HighPriorityListener, DiscoveryEventListener {
        /** */
        IgniteEx client;

        /** */
        public TestEventListener(IgniteEx client) {
            this.client = client;
        }

        /** */
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache cache) {
            if (((DiscoveryCustomEvent)evt).customMessage() instanceof ChangeGlobalStateMessage) {
                try {
                    assert GridTestUtils.waitForCondition(() -> client.context().state().clusterState().transition(), 10000)
                        : "Cluster state change is not in progress";

                    doSleep(2000);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        /** */
        @Override public int order() {
            return 0;
        }
    }
}
