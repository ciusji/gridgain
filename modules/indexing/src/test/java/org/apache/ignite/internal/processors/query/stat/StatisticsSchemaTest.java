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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.stat.config.IgniteStatisticsConfigurationManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 * Tests for statistics schema.
 */
public class StatisticsSchemaTest extends StatisticsStorageAbstractTest {
    /** */
    @Test
    public void updateStat() throws Exception {
        ((IgniteStatisticsManagerImpl)grid(0).context().query().getIndexing().statsManager())
            .statisticSchemaManager()
            .updateStatistics(
                Collections.singletonList(new StatisticsTarget("PUBLIC", "SMALL"))
            );

        U.sleep(500);

        ((IgniteStatisticsManagerImpl)grid(0).context().query().getIndexing().statsManager())
            .statisticSchemaManager()
            .updateStatistics(
                Collections.singletonList(new StatisticsTarget("PUBLIC", "SMALL"))
            );

        U.sleep(5000);

        startGrid(2);

//        checkStats();

        startGrid(3);

        U.sleep(1000);

        grid(0).cluster().setBaselineTopology(grid(0).context().discovery().topologyVersion());

        U.sleep(1000);

        checkStats();

//        System.out.println("+++ RESTART");
//
//        stopAllGrids();
//
//        U.sleep(500);
//
//        startGrid(0);
//
//        grid(0).context().query().querySqlFields(
//            new SqlFieldsQuery("CREATE TABLE TEST(ID INT PRIMARY KEY, VAL INT)"),
//            false
//        );
//
        U.sleep(5000);

    }

    /** */
    private void checkStats() {
        List<IgniteStatisticsManager> mgrs = G.allGrids().stream()
            .map(ign-> ((IgniteEx)ign).context().query().getIndexing().statsManager())
            .collect(Collectors.toList());

        System.out.println();
    }
}
