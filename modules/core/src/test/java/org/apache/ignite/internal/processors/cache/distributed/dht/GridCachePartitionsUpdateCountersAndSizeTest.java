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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.HashSet;
import java.util.regex.Pattern;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *For testing that partitions state validation works correctly and show partition size
 */
public class GridCachePartitionsUpdateCountersAndSizeTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** */
    private boolean clientMode;

    /** */
    private ListeningTestLogger testLog = new ListeningTestLogger(false, log());

    /** {@inheritDoc */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        cfg.setClientMode(clientMode);

        if (igniteInstanceName.endsWith("0"))
            cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *  Four tests that partitions state validation works correctly and show partition size always:
     *
     * Start three-nodes grid,
     * arguments: cnt - partition counters are inconsistent(boolean)
     *           size - partition size are inconsistent(boolean)
     * @throws Exception If failed.
     */
    private void startThreeNodesGrid(boolean cnt, boolean size) throws Exception {
        LogListener lsnrCnt = SizeCounterLogListener.matches(Pattern.compile
            ("Partitions update counters are inconsistent for")).build();

        LogListener lsnrSize = SizeCounterLogListener.matches(Pattern.compile
            ("Partitions cache sizes are inconsistent for")).build();

        LogListener lsnrSizeCnt = SizeCounterLogListener.matches(Pattern.compile
            ("Partitions cache size and update counters are inconsistent for")).build();

        IgniteEx ignite = startGrids(3);
        ignite.cluster().active(true);
        awaitPartitionMapExchange();

        testLog.clearListeners();

        testLog.registerListener(lsnrSize);
        testLog.registerListener(lsnrCnt);
        testLog.registerListener(lsnrSizeCnt);

        // Populate cache to increment update counters.
        for (int i = 0; i < 1000; i++)
            ignite.cache(CACHE_NAME).put(i, i);

        if (cnt) {
            // Modify update counter for some partition.
            ignite.cachex(CACHE_NAME).context().topology().localPartitions().get(0).updateCounter(100500L);
        }

        if (size) {
            // Modify update size for some partition.
            ignite.cachex(CACHE_NAME).context().topology().localPartitions().get(0).dataStore()
                .clear(ignite.cachex(CACHE_NAME).context().cacheId());
        }

        // Trigger exchange.
        startGrid(3);

        awaitPartitionMapExchange();

        // Nothing should happen (just log error message) and we're still able to put data to corrupted cache.
        ignite.cache(CACHE_NAME).put(0, 0);

        if (cnt && !size)
            assertTrue("Counters inconsistent message not found", lsnrCnt.check());

        if (!cnt && size)
            assertTrue("Size inconsistent message not found", lsnrSize.check());

        if (cnt && size)
            assertTrue("Both counters and sizes message not found", lsnrSizeCnt.check());

        if (!cnt && !size)
            assertFalse("Counters and Size inconsistent message found!", lsnrSize.check() && lsnrCnt.check()
                && lsnrSizeCnt.check());
    }

    /**
     *  1. Only partition counters are inconsistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValidationfPartitionCountersInconsistent() throws Exception {
        startThreeNodesGrid(true, false);
    }

    /**
     *  2. Only partition size are inconsistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValidationfPartitionSizeInconsistent() throws Exception {
        startThreeNodesGrid(false, true);
    }

    /**
     *  3. Partition counters and size are inconsistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValidationBothPartitionSizesAndCountersAreInconsistent() throws Exception {
        startThreeNodesGrid(true, true);
    }

    /**
     *  4. No one partition counters and size are inconsistent.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testValidationBothPatririonSixeAndCountersAreConsistent() throws Exception {
        startThreeNodesGrid(false, false);
    }

    /**
     *  Overraided class LogListener for find specific patterns.
     * @throws Exception If failed.
     */
    private static class SizeCounterLogListener extends LogListener {

        @Override public boolean check() {
            return false;
        }

        @Override public void reset() {

        }

        // Search specific string pattern and add value of counters and sizes to arrays
        @Override public void accept(String s) {
            HashSet<Long> setCnt = new HashSet<>();
            HashSet<Long> setSize = new HashSet<>();

            int typeInconsistence = 0; //for Cnt 1, for Size 2, for Both 3, no inconsisctence 0.

            if (s.matches("Partitions update counters are inconsistent for Part (\\[0-2]): " +
                "\\[dht.GridCachePartitionsUpdateCountersAndSizeTest(\\[0-2])=(32|100500) " +
                "dht.GridCachePartitionsUpdateCountersAndSizeTest(\\[0-2])=(32|100500) " +
                "dht.GridCachePartitionsUpdateCountersAndSizeTest(\\[0-2])=(32|100500) ]")) {
                typeInconsistence += 1;
                for (int i = 0; i < 3; i++) {
                    s = s.substring(s.indexOf('='));
                    setCnt.add(Long.parseLong(s.substring(0, s.indexOf(' ') - 1)));
                }
            }
            else if (s.matches("Partitions cache sizes are inconsistent for Part (\\[0-2]): " +
                "\\[dht.GridCachePartitionsUpdateCountersAndSizeTest(\\[0-2])=(32|0) " +
                "dht.GridCachePartitionsUpdateCountersAndSizeTest(\\[0-2])=(32|0}) " +
                "dht.GridCachePartitionsUpdateCountersAndSizeTest(\\[0-2])=(32|0}) ]")) {
                typeInconsistence += 10;
                for (int i = 0; i < 3; i++) {
                    s = s.substring(s.indexOf('='));
                    setSize.add(Long.parseLong(s.substring(0, s.indexOf(' ') - 1)));
                }
            }

            else if (s.matches("Partitions cache size and update counters are inconsistent for Part (\\[0-2]]): \\[" +
                "consistentId=dht.GridCachePartitionsUpdateCountersAndSizeTest(\\[0-2]) meta=\\[updCnt=(32|10500), size=(32|0)] " +
                "consistentId=dht.GridCachePartitionsUpdateCountersAndSizeTest(\\[0-2]) meta=\\[updCnt=(32|10500), size=(32|0)] " +
                "consistentId=dht.GridCachePartitionsUpdateCountersAndSizeTest(\\[0-2]) meta=\\[updCnt=(32|10500), size=(32|0)]")) {
                typeInconsistence = 100;
                for (int i=0; i<3; i++) {
                    s = s.substring(s.indexOf("size="));
                    setSize.add(Long.parseLong(s.substring(0, s.indexOf(' ') - 1)));
                    s = s.substring(s.indexOf("updCnt="));
                    setCnt.add(Long.parseLong(s.substring(0, s.indexOf(' ') - 1)));
                }
            }

            if (typeInconsistence == 1 && setCnt.size()==2)
                assertTrue("Counters inconsistent message found", true);
            else if (typeInconsistence == 10 && setSize.size()==2)
                assertTrue("Size inconsistent message found", true);
            else if (typeInconsistence == 100 && setCnt.size()==2 && setSize.size()==2)
                assertTrue("Size inconsistent message found", true);
        }
    }
}
