/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
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


package org.apache.ignite.yardstick.sql;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark for single PK.
 */

public class IgniteSinglePkIndexBenchmark {
    /** Key class and query to create TEST table. */
    private static final Map<Class<?>, String> setupQrys = new HashMap<>();

    private static final Map<Class<?>, Function<Integer, String>> whereBuilders = new HashMap<>();

    private static final Map<Class<?>, String> idxPk = new HashMap<>();

    static {
        setupQrys.put(
                IgniteSinglePkIndexBenchmark.TestKey.class,
                "CREATE TABLE TEST (ID0 INT, ID1 INT, VALINT INT, VALSTR VARCHAR, " +
                        "PRIMARY KEY (ID0)) " +
                        "WITH \"CACHE_NAME=TEST,KEY_TYPE=" + IgniteSinglePkIndexBenchmark.TestKey.class.getName() + ",VALUE_TYPE=" + IgniteCompositePkIndexBenchmark.Value.class.getName() + "\""
        );
        setupQrys.put(
                IgniteSinglePkIndexBenchmark.TestKeyHugeString.class,
                "CREATE TABLE TEST (ID0 VARCHAR, ID1 INT, VALINT INT, VALSTR VARCHAR, " +
                        "PRIMARY KEY (ID0)) " +
                        "WITH \"CACHE_NAME=TEST,KEY_TYPE=" + IgniteSinglePkIndexBenchmark.TestKeyHugeString.class.getName() + ",VALUE_TYPE=" + IgniteCompositePkIndexBenchmark.Value.class.getName() + "\""
        );

        idxPk.put(IgniteSinglePkIndexBenchmark.TestKey.class, "CREATE INDEX USER_PK ON TEST (ID0)");
        idxPk.put(IgniteSinglePkIndexBenchmark.TestKeyHugeString.class, "CREATE INDEX USER_PK ON TEST (ID0)");

        whereBuilders.put(IgniteSinglePkIndexBenchmark.TestKey.class, IgniteSinglePkIndexBenchmark::where2Int);
        whereBuilders.put(IgniteSinglePkIndexBenchmark.TestKeyHugeString.class, IgniteSinglePkIndexBenchmark::whereHugeStringAndInteger);
    }

    /** Cache name. */
    private static final String cacheName = "TEST";


    /** How many entries should be preloaded and within which range. */
    private int range;

    /** Key class. */
    private Class<?> keyCls;

    /** Value class. */
    private boolean addIndexes;

    /** */
    private Function<Integer, Object> keyCreator;

    /** */
    private IgniteCompositePkIndexBenchmark.TestAction testAct;

    /** */
    private Function<Integer, String> whereBuilder;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        range = args.range();
        String keyClsName = args.getStringParameter("keyClass", IgniteSinglePkIndexBenchmark.TestKey.class.getSimpleName());

        keyCls = Class.forName(IgniteSinglePkIndexBenchmark.class.getName() + "$" + keyClsName);
        final Constructor<?> keyConstructor = keyCls.getConstructor(int.class);

        keyCreator = (key) -> {
            try {
                return keyConstructor.newInstance(key);
            }
            catch (Exception e) {
                throw new IgniteException("Unexpected exception", e);
            }
        };

        addIndexes = args.getBooleanParameter("addIndexes", false);

        testAct = IgniteSinglePkIndexBenchmark.TestAction.valueOf(args.getStringParameter("action", "PUT").toUpperCase());
        whereBuilder = whereBuilders.get(keyCls);

        assert whereBuilder != null;

        printParameters();

        IgniteSemaphore sem = ignite().semaphore("setup", 1, true, true);

        try {
            if (sem.tryAcquire()) {
                println(cfg, "Create tables...");

                init();
            }
            else {
                // Acquire (wait setup by other client) and immediately release/
                println(cfg, "Waits for setup...");

                sem.acquire();
            }
        }
        finally {
            sem.release();
        }

        println("PLAN: " + sql("EXPLAIN SELECT ID1 FROM TEST WHERE VALINT=?", 1).getAll());
    }

    /**
     *
     */
    private void init() {
        String createTblQry = setupQrys.get(keyCls);

        assert createTblQry != null : "Invalid key class: " + keyCls;

        sql(createTblQry);

        sql("CREATE INDEX IDX_ID1 ON TEST(ID1)");

        if (testAct == IgniteCompositePkIndexBenchmark.TestAction.FIND_ONE)
            sql(idxPk.get(keyCls));

        if (addIndexes) {
            sql("CREATE INDEX IDX_VAL_INT ON TEST(VALINT)");
            sql("CREATE INDEX IDX_VAL_STR ON TEST(VALSTR)");
        }

        println(cfg, "Populate cache, range: " + range);

        try (IgniteDataStreamer<Object, Object> stream = ignite().dataStreamer(cacheName)) {
            stream.allowOverwrite(false);

            for (int k = 0; k < range; ++k)
                stream.addData(keyCreator.apply(k), new IgniteCompositePkIndexBenchmark.Value(k));
        }

        println(cfg, "Cache populated. ");
        List<?> row = sql("SELECT * FROM TEST LIMIT 1").getAll().get(0);

        println("    partitions: " + ((IgniteEx)ignite()).cachex("TEST").affinity().partitions());
        println(cfg, "TEST table row: \n" + row);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        switch (testAct) {
            case PUT: {
                int k = ThreadLocalRandom.current().nextInt(range);
                int v = ThreadLocalRandom.current().nextInt(range);

                ignite().cache(cacheName).put(keyCreator.apply(k), new IgniteCompositePkIndexBenchmark.Value(v));

                return true;
            }

            case SCAN: {
                int k = ThreadLocalRandom.current().nextInt(range);

                List<List<?>> res = sql("SELECT ID1 FROM TEST WHERE VALINT=?", k).getAll();

                assert res.size() == 1;

                return true;
            }

            case CACHE_SCAN: {
                int k = ThreadLocalRandom.current().nextInt(range);

                ScanQuery<BinaryObject, BinaryObject> qry = new ScanQuery<>();

                qry.setFilter(new IgniteCompositePkIndexBenchmark.Filter(k));

                IgniteCache<BinaryObject, BinaryObject> cache = ignite().cache(cacheName).withKeepBinary();

                List<IgniteCache.Entry<BinaryObject, BinaryObject>> res = cache.query(qry).getAll();

                if (res.size() != 1)
                    throw new Exception("Invalid result size: " + res.size());

                if ((int)res.get(0).getValue().field("valInt") != k)
                    throw new Exception("Invalid entry found [key=" + k + ", entryKey=" + res.get(0).getKey() + ']');

                return true;
            }

            case FIND_ONE: {
                int k = ThreadLocalRandom.current().nextInt(range);

                List<List<?>> res = sql("SELECT ID1 FROM TEST WHERE " + whereBuilder.apply(k)).getAll();

                assert res.size() == 1;

                return true;
            }

            default:
                assert false : "Invalid action: " + testAct;

                return false;
        }
    }

    /** */
    private static  String where2Int(int k) {
        return "ID1 = " + k;
    }

    /** */
    private static String whereHugeStringAndInteger(int k) {
        return "ID0 = '" + IgniteSinglePkIndexBenchmark.TestKeyHugeString.PREFIX + k + "' AND ID1 = " + k;
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return ((IgniteEx)ignite()).context().query().querySqlFields(new SqlFieldsQuery(sql)
                .setSchema("PUBLIC")
                .setLazy(true)
                .setEnforceJoinOrder(true)
                .setArgs(args), false);
    }
    /** */
    private void printParameters() {
        println("Benchmark parameter:");
        println("    range: " + range);
        println("    key: " + keyCls.getSimpleName());
        println("    idxs: " + addIndexes);
        println("    action: " + testAct);
    }

    /** */
    public static class TestKey {
        /** */
        @QuerySqlField
        private final int id0;

        /** */
        public TestKey2Integers(int key) {
            this.id0 = key;
        }
    }

    /** */
    public static class TestKeyHugeString {
        /** Prefix. */
        static final String PREFIX = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

        /** */
        @QuerySqlField
        private final String id0;

        /** */
        public TestKeyHugeStringAndInteger(int key) {
            this.id0 = PREFIX + key;
        }
    }

    /** */
    public static class Value {
        /** */
        @QuerySqlField
        private final int valInt;

        @QuerySqlField
        private final String valStr;

        /** */
        public Value(int key) {
            this.valInt = key;
            this.valStr = "val_str" + key;
        }
    }

    /**
     *
     */
    static class Filter implements IgniteBiPredicate<BinaryObject, BinaryObject> {
        /** */
        private final int val;

        /**
         * @param val Value to find.
         */
        public Filter(Integer val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(BinaryObject key, BinaryObject val) {
            return this.val == (int)val.field("valInt");
        }
    }

    /** */
    public enum TestAction {
        /** */
        PUT,

        /** */
        SCAN,

        /** */
        CACHE_SCAN,

        /** */
        FIND_ONE
    }

}
