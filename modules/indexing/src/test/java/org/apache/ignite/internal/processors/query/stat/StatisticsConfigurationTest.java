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
package org.apache.ignite.internal.processors.query.stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for statistics configuration.
 */
@RunWith(Parameterized.class)
public class StatisticsConfigurationTest extends StatisticsAbstractTest {
    /** Statistics await timeout.*/
    private static final long STAT_TIMEOUT = 5_000;

    /** Columns to check.*/
    private static final String[] COLUMNS = {"A", "B", "C"};

    /** Lazy mode. */
    @Parameterized.Parameter(value = 0)
    public boolean persist;

    /** */
    @Parameterized.Parameters(name = "persist={0}")
    public static List<Object[]> parameters() {
        ArrayList<Object[]> params = new ArrayList<>();

        boolean[] arrBool = new boolean[] {true, false};

        for (boolean persist0 : arrBool)
            params.add(new Object[] {persist0});

        return params;
    }

    /** Statistic checker: total row count. */
    private Consumer<List<ObjectStatisticsImpl>> checkTotalRows = stats -> {
        long rows = stats.stream()
            .mapToLong(s -> {
                assertNotNull(s);

                return s.rowCount();
            })
            .sum();

        assertEquals(SMALL_SIZE, rows);
    };

    /** Statistic checker. */
    private Consumer<List<ObjectStatisticsImpl>> checkColumStats = stats -> {
        for (ObjectStatisticsImpl stat : stats) {
            for (String col : COLUMNS) {
                ColumnStatistics colStat = stat.columnStatistics(col);
                assertNotNull("Column: " + col, colStat);

                assertTrue("Column: " + col, colStat.cardinality() > 0);
                assertTrue("Column: " + col, colStat.max().getInt() > 0);
                assertTrue("Column: " + col, colStat.total() == stat.rowCount());
            }
        }
    };

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(persist)
                    )
            );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx startGrid(int nodeIdx) throws Exception {
        System.out.println("+++ START " + nodeIdx);

        IgniteEx ign = super.startGrid(nodeIdx);

        ign.cluster().state(ClusterState.ACTIVE);

        if (persist)
            ign.cluster().setBaselineTopology(ign.cluster().topologyVersion());

        awaitPartitionMapExchange();

        return ign;
    }


    /** {@inheritDoc} */
    @Override protected void stopGrid(int nodeIdx) {
        System.out.println("+++ STOP " + nodeIdx);

        super.stopGrid(nodeIdx);

        if (persist)
            F.first(G.allGrids()).cluster().setBaselineTopology(F.first(G.allGrids()).cluster().topologyVersion());

        try {
            awaitPartitionMapExchange();
        }
        catch (InterruptedException e) {
            // No-op.
        }
    }

    /** */
    @Test
    public void updateStatisticsOnChangeTopology() throws Exception {
        startGrid(0);

        createSmallTable("");

        updateStatistics(new StatisticsTarget("PUBLIC", "SMALL"));

        waitForStats("PUBLIC", "SMALL", STAT_TIMEOUT, checkTotalRows, checkColumStats);

        startGrid(1);

        waitForStats("PUBLIC", "SMALL", STAT_TIMEOUT, checkTotalRows, checkColumStats);

        startGrid(2);

        waitForStats("PUBLIC", "SMALL", STAT_TIMEOUT, checkTotalRows, checkColumStats);

        startGrid(3);

        waitForStats("PUBLIC", "SMALL", STAT_TIMEOUT, checkTotalRows, checkColumStats);

        stopGrid(0);

        waitForStats("PUBLIC", "SMALL", STAT_TIMEOUT, checkTotalRows, checkColumStats);

        stopGrid(2);

        waitForStats("PUBLIC", "SMALL", STAT_TIMEOUT, checkTotalRows, checkColumStats);

        stopGrid(3);

        waitForStats("PUBLIC", "SMALL", STAT_TIMEOUT, checkTotalRows, checkColumStats);

        stopGrid(3);

        waitForStats("PUBLIC", "SMALL", STAT_TIMEOUT, checkTotalRows, checkColumStats);
    }

    /** */
    @Test
    public void dropUpdate() throws Exception {
        startGrids(3);

        createSmallTable("");

        updateStatistics(new StatisticsTarget("PUBLIC", "SMALL"));

        waitForStats("PUBLIC", "SMALL", STAT_TIMEOUT, checkTotalRows, checkColumStats);

        grid(0).context().query().getIndexing().statsManager()
            .dropStatistics(new StatisticsTarget("PUBLIC", "SMALL"));

        waitForStats("PUBLIC", "SMALL", STAT_TIMEOUT, (stats) -> stats.forEach(s -> assertNull(s)));

        updateStatistics(new StatisticsTarget("PUBLIC", "SMALL"));

        waitForStats("PUBLIC", "SMALL", STAT_TIMEOUT, checkTotalRows, checkColumStats);
    }

    /** */
    private void waitForStats(
        String schema,
        String objName,
        long timeout,
        Consumer<List<ObjectStatisticsImpl>>... statsCheckers
    ) {
        long t0 = U.currentTimeMillis();

        while (true) {
            try {
                List<IgniteStatisticsManager> mgrs = G.allGrids().stream()
                    .map(ign -> ((IgniteEx)ign).context().query().getIndexing().statsManager())
                    .collect(Collectors.toList());

                List<ObjectStatisticsImpl> stats = mgrs.stream()
                    .map(m -> (ObjectStatisticsImpl)m.getLocalStatistics(schema, objName))
                    .collect(Collectors.toList());

                for (Consumer<List<ObjectStatisticsImpl>> statChecker : statsCheckers)
                    statChecker.accept(stats);

                return;
            } catch (Throwable ex) {
                if (t0 + timeout < U.currentTimeMillis())
                    throw ex;
                else {
                    try {
                        U.sleep(200);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        // No-op.
                    }
                }
            }
        }
    }
}
