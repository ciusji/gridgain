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

package org.apache.ignite.spi.communication.tcp;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.communication.IgniteIoTestMessage;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.internal.TcpInverseConnectionResponseMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;

/**
 * Tests for communication over discovery feature (inverse communication request).
 */
public class GridTcpCommunicationInverseConnectionEstablishingTest extends GridCommonAbstractTest {

    /** */
    private static final String UNREACHABLE_IP = "172.31.30.132";

    /** */
    private static final String UNRESOLVED_HOST = "unresolvedHost";

    /** */
    private static final String CACHE_NAME = "cache-0";

    /** */
    private static final AtomicReference<String> UNREACHABLE_DESTINATION = new AtomicReference<>();

    /** Allows to make client not to respond to inverse connection request. */
    private static final AtomicBoolean RESPOND_TO_INVERSE_REQUEST = new AtomicBoolean(true);

    /** */
    private static final int SRVS_NUM = 2;

    /** */
    private boolean forceClientToSrvConnections;

    /** */
    private CacheConfiguration ccfg;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        forceClientToSrvConnections = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(8_000);

        cfg.setCommunicationSpi(
            new TestCommunicationSpi()
                .setForceClientToServerConnections(forceClientToSrvConnections)
        );

        if (ccfg != null) {
            cfg.setCacheConfiguration(ccfg);

            ccfg = null;
        }

        return cfg;
    }

    /**
     * Verifies that server successfully connects to "unreachable" client with
     * {@link TcpCommunicationSpi#forceClientToServerConnections()}} flag.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUnreachableClientInVirtualizedEnvironment() throws Exception {
        UNREACHABLE_DESTINATION.set(UNREACHABLE_IP);
        RESPOND_TO_INVERSE_REQUEST.set(true);

        executeCacheTestWithUnreachableClient(true);
    }

    /**
     * Verifies that server successfully connects to "unreachable" client with
     * {@link TcpCommunicationSpi#forceClientToServerConnections()}} flag.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUnreachableClientInStandAloneEnvironment() throws Exception {
        UNREACHABLE_DESTINATION.set(UNREACHABLE_IP);
        RESPOND_TO_INVERSE_REQUEST.set(true);

        executeCacheTestWithUnreachableClient(false);
    }

    /**
     * Verifies that server successfully connects to client provided unresolvable host in virtualized environment.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientWithUnresolvableHostInVirtualizedEnvironment() throws Exception {
        UNREACHABLE_DESTINATION.set(UNRESOLVED_HOST);
        RESPOND_TO_INVERSE_REQUEST.set(true);

        executeCacheTestWithUnreachableClient(true);
    }

    /**
     * Verifies that server successfully connects to client provided unresolvable host in stand-alone environment.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientWithUnresolvableHostInStandAloneEnvironment() throws Exception {
        UNREACHABLE_DESTINATION.set(UNRESOLVED_HOST);
        RESPOND_TO_INVERSE_REQUEST.set(true);

        executeCacheTestWithUnreachableClient(false);
    }

    /**
     * Verify that inverse connection can be established if client reconnects to another router server with the same id.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnectDuringInverseConnection() throws Exception {
        UNREACHABLE_DESTINATION.set(UNRESOLVED_HOST);
        RESPOND_TO_INVERSE_REQUEST.set(true);

        Assume.assumeThat(System.getProperty("zookeeper.forceSync"), is(nullValue()));

        startGrid(0).cluster().state(ClusterState.ACTIVE);

        startGridWithCfg(1, cfg -> {
            cfg.setClientMode(true);

            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(new TcpDiscoveryVmIpFinder(false)
                .setAddresses(
                    Collections.singletonList("127.0.0.1:47500..47502") // "47501" is a port of the client itself.
                )
            );

            return cfg;
        });

        AtomicBoolean msgRcvd = new AtomicBoolean();

        grid(1).context().io().addMessageListener(GridTopic.TOPIC_IO_TEST, (nodeId, msg, plc) -> {
            msgRcvd.set(true);
        });

        UUID clientNodeId = grid(1).context().localNodeId();
        UUID oldRouterNode = ((TcpDiscoveryNode)grid(1).localNode()).clientRouterNodeId();

        startGrid(2);

        startGrid(3);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            ClusterNode clientNode = grid(3).context().discovery().node(clientNodeId);

            grid(3).context().io().sendIoTest(clientNode, new byte[10], false);
        });

        doSleep(2000L); // Client failover timeout is 8 seconds.

        stopGrid(0);

        fut.get(8000L);

        UUID newId = grid(1).localNode().id();
        UUID newRouterNode = ((TcpDiscoveryNode)grid(1).localNode()).clientRouterNodeId();

        assertEquals(clientNodeId, newId);
        assertFalse(oldRouterNode + " " + newRouterNode, newRouterNode.equals(oldRouterNode));

        assertTrue(GridTestUtils.waitForCondition(msgRcvd::get, 1000L));
    }

    /**
     * Verify that inverse connection request won't hang cluster in case of paired connections enabled.
     *
     * Description:
     * 1. Start server node that won't be able to reach server.
     * 2. Start client node.
     * 3. Send message from server node to client node waiting for server node to start creating communication client
     * thus making sure that second operation of sending message will try to wait on Future created for communication client.
     * 4. Both operations must fail and not hang.
     * @throws Exception If failed.
     */
    @Test
    public void testPairedAndInverseConnectionDoesntHang() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL, "true");

        final int fails = 2;

        UNREACHABLE_DESTINATION.set(UNRESOLVED_HOST);

        AtomicBoolean enableBarrier = new AtomicBoolean(false);

        CountDownLatch messageLatch = new CountDownLatch(fails);
        CountDownLatch clientLatch = new CountDownLatch(fails);

        class LatchedCommunicationSpi extends TestCommunicationSpi {

            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
                if (node.isClient() && enableBarrier.get()) {
                    try {
                        messageLatch.countDown();
                        messageLatch.await();

                        clientLatch.countDown();
                        clientLatch.await();
                    } catch (InterruptedException ignored) {
                    }
                }
                super.sendMessage(node, msg, ackC);
            }

            @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
                if (node.isClient()) {
                    try {
                        enableBarrier.set(true);
                        clientLatch.await();
                    } catch (InterruptedException ignored) {
                    }
                }
                return super.createTcpClient(node, connIdx);
            }
        }

        Assume.assumeThat(System.getProperty("zookeeper.forceSync"), is(nullValue()));

        final IgniteEx server = startGridWithCfg(0, cfg -> {
            cfg.setCommunicationSpi(new LatchedCommunicationSpi().setUsePairedConnections(true));
            return cfg;
        });

        server.cluster().state(ClusterState.ACTIVE);

        final int clientIdx = 1;

        GridTestUtils.runAsync(() -> {
            try {
                startGridWithCfg(clientIdx, cfg -> {
                    cfg.setClientMode(true);

                    ((TcpDiscoverySpi) cfg.getDiscoverySpi()).setIpFinder(new TcpDiscoveryVmIpFinder(false)
                            .setAddresses(
                                    Collections.singletonList("127.0.0.1:47500..47502") // "47501" is a port of the client itself.
                            )
                    );

                    final LatchedCommunicationSpi spi = new LatchedCommunicationSpi();

                    cfg.setCommunicationSpi(spi);

                    spi.setUsePairedConnections(true);
                    spi.setForceClientToServerConnections(forceClientToSrvConnections);

                    return cfg;
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // wait until client node is (partially) available
        assertTrue(GridTestUtils.waitForCondition(() -> IgnitionEx.allGridsx().size() == 2, 60_000));

        // get client node
        assertTrue(GridTestUtils.waitForCondition(() -> {
            final List<Ignite> ignites = IgnitionEx.allGridsx();
            return ignites.stream()
                .map(ignite -> (IgniteEx) ignite)
                .anyMatch(ignite -> ignite.context() != null && ignite.context().clientNode());
        }, 60_000));

        final List<Ignite> ignites = IgnitionEx.allGridsx();

        final IgniteEx client = ignites.stream()
            .map(ignite -> (IgniteEx) ignite)
            .filter(ignite -> ignite.context() != null && ignite.context().clientNode())
            .findFirst().get();

        final UUID clientId = client.context().localNodeId();

        // wait until client node is visible for server node
        assertTrue(GridTestUtils.waitForCondition(() -> {
            return server.context().discovery().discoCache() != null && server.context().discovery().node(clientId) != null;
        }, 60_000));

        final ClusterNode clientNode = server.context().discovery().node(clientId);

        AtomicInteger failCount = new AtomicInteger(0);

        final GridIoManager serverGridIo = server.context().io();

        // send two messages from server to client, making it wait on communication client future
        for (int i = 0; i < fails; i++) {
            GridTestUtils.runAsync(() -> {
                final IgniteInternalFuture<List<IgniteIoTestMessage>> future = serverGridIo.sendIoTest(clientNode, new byte[]{1, 2, 3, 4}, false);
                try {
                    future.get();
                } catch (IgniteCheckedException e) {
                    failCount.incrementAndGet();
                }
            });
        }

        assertTrue(GridTestUtils.waitForCondition(() -> failCount.get() == fails, TimeUnit.SECONDS.toMillis(60)));

        server.close();

        List<Thread> asyncRunnables = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().contains("async-runnable-runner"))
                .collect(Collectors.toList());

        for (Thread asyncRunnable : asyncRunnables) {
            U.interrupt(asyncRunnable);

            U.join(asyncRunnable, log);
        }

        IgnitionEx.stopAll(true, null);
    }

    /**
     * Executes cache test with "unreachable" client.
     *
     * @param forceClientToSrvConnections Flag for the client mode.
     * @throws Exception If failed.
     */
    private void executeCacheTestWithUnreachableClient(boolean forceClientToSrvConnections) throws Exception {
        LogListener lsnr = LogListener.matches("Failed to send message to remote node").atMost(0).build();

        for (int i = 0; i < SRVS_NUM; i++) {
            ccfg = cacheConfiguration(CACHE_NAME, ATOMIC);

            startGridWithCfg(i, cfg -> {
                ListeningTestLogger log = new ListeningTestLogger(false, cfg.getGridLogger());

                log.registerListener(lsnr);

                return cfg.setGridLogger(log);
            });
        }

        this.forceClientToSrvConnections = forceClientToSrvConnections;

        startClientGrid(SRVS_NUM);

        putAndCheckKey();

        assertTrue(lsnr.check());
    }

    /**
     * No server threads hang even if client doesn't respond to inverse connection request.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientSkipsInverseConnectionResponse() throws Exception {
        UNREACHABLE_DESTINATION.set(UNRESOLVED_HOST);
        RESPOND_TO_INVERSE_REQUEST.set(false);

        startGrids(SRVS_NUM - 1);

        LogListener lsnr = LogListener.matches(
            "Failed to wait for establishing inverse communication connection"
        ).build();

        startGridWithCfg(SRVS_NUM - 1, cfg -> {
            ListeningTestLogger log = new ListeningTestLogger(false, cfg.getGridLogger());

            log.registerListener(lsnr);

            return cfg.setGridLogger(log);
        });

        forceClientToSrvConnections = false;

        IgniteEx client = startClientGrid(SRVS_NUM);
        ClusterNode clientNode = client.localNode();

        IgniteEx srv = grid(SRVS_NUM - 1);

        // We need to interrupt communication worker client nodes so that
        // closed connection won't automatically reopen when we don't expect it.
        // Server communication worker is interrupted for another reason - it can hang the test
        // due to bug in inverse connection protocol & comm worker - it will be fixed later.
        List<Thread> tcpCommWorkerThreads = Thread.getAllStackTraces().keySet().stream()
            .filter(t -> t.getName().contains("tcp-comm-worker"))
            .filter(t -> /*t.getName().contains(srv.name()) || */t.getName().contains(client.name()))
            .collect(Collectors.toList());

        for (Thread tcpCommWorkerThread : tcpCommWorkerThreads) {
            U.interrupt(tcpCommWorkerThread);

            U.join(tcpCommWorkerThread, log);
        }

        TcpCommunicationSpi spi = (TcpCommunicationSpi)srv.configuration().getCommunicationSpi();

        GridTestUtils.invoke(spi, "onNodeLeft", clientNode.consistentId(), clientNode.id());

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() ->
            srv.context().io().sendIoTest(clientNode, new byte[10], false).get()
        );

        assertTrue(GridTestUtils.waitForCondition(fut::isDone, 30_000));

        assertTrue(lsnr.check());
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    protected final CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * Puts a key to a server that is backup for the key and doesn't have an open communication connection to client.
     * This forces the server to establish a connection to "unreachable" client.
     */
    private void putAndCheckKey() {
        int key = 0;
        IgniteEx srv2 = grid(SRVS_NUM - 1);

        for (int i = 0; i < 1_000; i++) {
            if (srv2.affinity(CACHE_NAME).isBackup(srv2.localNode(), i)) {
                key = i;

                break;
            }
        }

        IgniteEx cl0 = grid(SRVS_NUM);

        IgniteCache<Object, Object> cache = cl0.cache(CACHE_NAME);

        cache.put(key, key);
        assertEquals(key, cache.get(key));
    }

    /** */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
            if (node.isClient()) {
                Map<String, Object> attrs = new HashMap<>(node.attributes());

                attrs.put(createAttributeName(ATTR_ADDRS), Collections.singleton(UNREACHABLE_DESTINATION.get()));
                attrs.put(createAttributeName(ATTR_PORT), 47200);
                attrs.put(createAttributeName(ATTR_EXT_ADDRS), Collections.emptyList());
                attrs.put(createAttributeName(ATTR_HOST_NAMES), Collections.emptyList());

                ((TcpDiscoveryNode)(node)).setAttributes(attrs);
            }

            return super.createTcpClient(node, connIdx);
        }

        /**
         * @param name Name.
         */
        private String createAttributeName(String name) {
            return getClass().getSimpleName() + '.' + name;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                GridIoMessage msg0 = (GridIoMessage)msg;

                if (msg0.message() instanceof TcpInverseConnectionResponseMessage && !RESPOND_TO_INVERSE_REQUEST.get()) {
                    log.info("Client skips inverse connection response to server: " + node);

                    return;
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
