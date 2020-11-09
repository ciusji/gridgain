package org.apache.ignite.internal.processors.query.stat;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Basic tests of value distribution statistics usage.
 */
@RunWith(Parameterized.class)
public class BasicValueDistributionTableStatisticsUsageTest extends TableStatisticsAbstractTest {
    /** */
    @Parameterized.Parameter(0)
    public CacheMode cacheMode;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "cacheMode={0}")
    public static Collection parameters() {
        return Arrays.asList(new Object[][] {
                { REPLICATED },
                { PARTITIONED },
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGridsMultiThreaded(2);

        node.getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        runSql("DROP TABLE IF EXISTS digital_distribution");

        runSql("CREATE TABLE digital_distribution (ID INT PRIMARY KEY, col_a int, col_b int, col_c int, col_d int) " +
                "WITH \"TEMPLATE=" + cacheMode + "\"");

        runSql("CREATE INDEX digital_distribution_col_a ON digital_distribution(col_a)");
        runSql("CREATE INDEX digital_distribution_col_b ON digital_distribution(col_b)");
        runSql("CREATE INDEX digital_distribution_col_c ON digital_distribution(col_c)");
        runSql("CREATE INDEX digital_distribution_col_d ON digital_distribution(col_d)");

        for (int i = 0; i < 100; i++) {
            String sql = String.format("INSERT INTO digital_distribution(id, col_a, col_b, col_c, col_d)" +
                    " VALUES(%d,%d, %d, 1, null)", i, i, i + 200);
            runSql(sql);
        }
        runSql("INSERT INTO digital_distribution(id, col_a, col_b, col_c) VALUES(101, null, 301, null)");

        runSql("DROP TABLE IF EXISTS empty_distribution");

        runSql("CREATE TABLE empty_distribution (ID INT PRIMARY KEY, col_a int) " +
                "WITH \"TEMPLATE=" + cacheMode + "\"");

        runSql("CREATE INDEX empty_distribution_col_a ON empty_distribution(col_a)");

        runSql("DROP TABLE IF EXISTS empty_distribution_no_stat");

        runSql("CREATE TABLE empty_distribution_no_stat (ID INT PRIMARY KEY, col_a int) " +
                "WITH \"TEMPLATE=" + cacheMode + "\"");

        runSql("CREATE INDEX empty_distribution_no_stat_col_a ON empty_distribution_no_stat(col_a)");

        updateStatistics("digital_distribution", "empty_distribution");
    }

    /**
     * Select with two conditions with border higher than all values in one of them and check that
     * that column index will be used.
     */
    @Test public void selectOverhightBorder() {
        String sql = "select count(*) from digital_distribution i1 where col_a > 200 and col_b > 200";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_A"}, sql,
                new String[1][]);
    }

    /**
     * Select with two conditions with border lower than all values in one of them and check that
     * that column index will be used.
     */
    @Test public void selectOverlowBorder() {
        String sql = "select count(*) from digital_distribution i1 where col_a < 200 and col_b < 200";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_B"}, sql,
                new String[1][]);
    }

    /**
     * Select with two is null condition and check that will be used index by column without nulls.
     */
    @Test public void selectNull() {
        String sql = "select count(*) from digital_distribution i1 where col_a is null and col_b is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_B"}, sql,
                new String[1][]);
    }

    /**
     * Select with "higher" clause from column with all the same values.
     */
    @Test public void selectHigherFromSingleValue() {
        String sql = "select count(*) from digital_distribution i1 where col_c > 1";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_C"}, sql,
                new String[1][]);
    }

    /**
     * Select with "lower" clause from column with all the same values.
     */
    @Test public void selectLowerToSingleValue() {
        String sql = "select count(*) from digital_distribution i1 where col_c > 1";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_C"}, sql,
                new String[1][]);
    }

    /**
     * Select with "is null" clause from column with only null values.
     */
    @Test public void selectNullFromNull() {
        String sql = "select count(*) from digital_distribution i1 where col_d is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_D"}, sql,
                new String[1][]);
    }

    /**
     * Select with "greater" clause from column with only null values.
     */
    @Test public void selectGreaterFromNull() {
        String sql = "select count(*) from digital_distribution i1 where col_d > 0";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_D"}, sql,
                new String[1][]);
    }

    /**
     * Select with "is null" clause from empty table.
     */
    @Test public void selectLessOrEqualFromNull() {
        String sql = "select count(*) from digital_distribution i1 where col_d <= 1000";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"DIGITAL_DISTRIBUTION_COL_D"}, sql,
                new String[1][]);
    }

    /**
     * Select with "less or equal" clause from empty table without statistics.
     */
    @Test public void selectFromEmptyNoStatTable() {
        String sql = "select count(*) from empty_distribution_no_stat i1 where col_a <= 1000";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"EMPTY_DISTRIBUTION_NO_STAT_COL_A"}, sql,
                new String[1][]);
    }

    /**
     * Select with "is null" clause from empty table without statistics.
     */
    @Test public void selectNullFromEmptyNoStatTable() {
        String sql = "select count(*) from empty_distribution_no_stat i1 where col_a is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"EMPTY_DISTRIBUTION_NO_STAT_COL_A"}, sql,
                new String[1][]);
    }

    /**
     * Select with "is not null" clause from empty table without statistics.
     */
    @Test public void selectNotNullFromEmptyNoStatTable() {
        String sql = "select count(*) from empty_distribution_no_stat i1 where col_a is not null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{}, sql, new String[1][]);
    }

    /**
     * Select with "less or equal" clause from empty table.
     */
    @Test public void selectFromEmptyTable() {
        String sql = "select count(*) from empty_distribution i1 where col_a <= 1000";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"EMPTY_DISTRIBUTION_COL_A"}, sql,
                new String[1][]);
    }

    /**
     * Select with "is null" clause from empty table.
     */
    @Test public void selectNullFromEmptyTable() {
        String sql = "select count(*) from empty_distribution i1 where col_a is null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{"EMPTY_DISTRIBUTION_COL_A"}, sql,
                new String[1][]);
    }

    /**
     * Select with "is not null" clause from empty table.
     */
    @Test public void selectNotNullFromEmptyTable() {
        String sql = "select count(*) from empty_distribution i1 where col_a is not null";
        checkOptimalPlanChosenForDifferentIndexes(grid(0), new String[]{}, sql, new String[1][]);
    }
}
