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

package org.apache.ignite.yardstick.sql;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs query operations with joins.
 */
public class IgniteSqlHashJoinBenchmark extends IgniteAbstractBenchmark {
    /** Queries. */
    private final Map<String, String> qrys = new HashMap<>();

    /** Sql query for benchmark. */
    private String sql;

    /** Is random parameter is required for the query. */
    private boolean firstParam;

    /** Is random parameter is required for the query. */
    private boolean secondParam;

    /** Parameter range. */
    private long firstParamRange;

    /** Range. */
    private long range;

    /** Initialize step. */
    private boolean initStep;

    /** Create index step. */
    private boolean createIndexStep;

    // Fill query map
    {
        qrys.put("SQL_HJ_3_TBLS",
            "SELECT D.VAL, F0.VAL, F1.VAL " +
                "FROM DATA_TBL AS D, FACT0 AS F0 USE INDEX (HASH_JOIN_IDX), FACT1 AS F1 USE INDEX (HASH_JOIN_IDX) " +
                "WHERE D.FACT0_ID=F0.FACT_ID AND D.FACT1_ID_STR=F1.FACT_ID_STR");

        qrys.put("SQL_HJ_2_TBLS_BY_LONG",
            "SELECT D.VAL, F0.VAL " +
                "FROM DATA_TBL AS D, FACT0 AS F0 USE INDEX (HASH_JOIN_IDX) " +
                "WHERE D.FACT0_ID=F0.FACT_ID");

        qrys.put("SQL_HJ_2_TBLS_BY_STR",
            "SELECT D.VAL, F1.VAL " +
                "FROM DATA_TBL AS D, FACT1 AS F1 USE INDEX (HASH_JOIN_IDX) " +
                "WHERE D.FACT1_ID_STR=F1.FACT_ID_STR");

        qrys.put("SQL_NL_3_TBLS",
            "SELECT D.VAL, F0.VAL, F1.VAL " +
                "FROM FACT0 AS F0, DATA_TBL AS D, FACT1 AS F1 " +
                "WHERE D.FACT0_ID=F0.FACT_ID AND D.FACT1_ID_STR=F1.FACT_ID_STR");

        qrys.put("SQL_NL_2_TBLS_BY_LONG",
            "SELECT D.VAL, F0.VAL " +
                "FROM FACT0 AS F0, DATA_TBL AS D " +
                "WHERE D.FACT0_ID=F0.FACT_ID");

        qrys.put("SQL_NL_2_TBLS_BY_STR",
            "SELECT D.VAL, F1.VAL " +
                "FROM FACT1 AS F1, DATA_TBL AS D " +
                "WHERE D.FACT1_ID_STR=F1.FACT_ID_STR");

        // HJ with WHERE clause
        qrys.put("SQL_HJ_3_TBLS_FILTERED_BY_IDX",
            "SELECT D.VAL, F0.VAL, F1.VAL " +
                "FROM DATA_TBL AS D, FACT0 AS F0 USE INDEX (HASH_JOIN_IDX), FACT1 AS F1 USE INDEX (HASH_JOIN_IDX) " +
                "WHERE D.FACT0_ID=F0.FACT_ID AND D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.IDX_ID >=? AND D.IDX_ID < ?");

        qrys.put("SQL_HJ_3_TBLS_FILTERED_BY_NOIDX",
            "SELECT D.VAL, F0.VAL, F1.VAL " +
                "FROM DATA_TBL AS D, FACT0 AS F0 USE INDEX (HASH_JOIN_IDX), FACT1 AS F1 USE INDEX (HASH_JOIN_IDX) " +
                "WHERE D.FACT0_ID=F0.FACT_ID AND D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.NO_IDX_ID >=? AND D.NO_IDX_ID < ?");

        // NL with WHERE clause Nested loops different JOIN ORDER
        qrys.put("SQL_NL_3_TBLS_JO1_FILTERED_BY_IDX",
            "SELECT D.VAL, F0.VAL, F1.VAL " +
                "FROM DATA_TBL AS D, FACT0 AS F0, FACT1 AS F1 " +
                "WHERE D.FACT0_ID=F0.FACT_ID AND D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.IDX_ID >=? AND D.IDX_ID < ?");

        qrys.put("SQL_NL_3_TBLS_JO2_FILTERED_BY_IDX",
            "SELECT D.VAL, F0.VAL, F1.VAL " +
                "FROM FACT0 AS F0, DATA_TBL AS D, FACT1 AS F1 " +
                "WHERE D.FACT0_ID=F0.FACT_ID AND D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.IDX_ID >=? AND D.IDX_ID < ?");

        qrys.put("SQL_NL_3_TBLS_JO1_FILTERED_BY_NOIDX",
            "SELECT D.VAL, F0.VAL, F1.VAL " +
                "FROM DATA_TBL AS D, FACT0 AS F0, FACT1 AS F1 " +
                "WHERE D.FACT0_ID=F0.FACT_ID AND D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.NO_IDX_ID >=? AND D.NO_IDX_ID < ?");

        qrys.put("SQL_NL_3_TBLS_JO2_FILTERED_BY_NOIDX",
            "SELECT D.VAL, F0.VAL, F1.VAL " +
                "FROM FACT0 AS F0, DATA_TBL AS D, FACT1 AS F1 " +
                "WHERE D.FACT0_ID=F0.FACT_ID AND D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.NO_IDX_ID >=? AND D.NO_IDX_ID < ?");

        // 2 tables filtered, without HASH
        qrys.put("SQL_NL_2_TBLS_BY_LONG_JO0_FILTERED_BY_IDX",
            "SELECT D.VAL, F0.VAL " +
                "FROM FACT0 AS F0, DATA_TBL AS D " +
                "WHERE D.FACT0_ID=F0.FACT_ID " +
                "AND D.IDX_ID >=? AND D.IDX_ID < ?");
        qrys.put("SQL_NL_2_TBLS_BY_LONG_JO1_FILTERED_BY_IDX",
            "SELECT D.VAL, F0.VAL " +
                "FROM DATA_TBL AS D, FACT0 AS F0 " +
                "WHERE D.FACT0_ID=F0.FACT_ID " +
                "AND D.IDX_ID >=? AND D.IDX_ID < ?");

        qrys.put("SQL_NL_2_TBLS_BY_LONG_JO0_FILTERED_BY_NOIDX",
            "SELECT D.VAL, F0.VAL " +
                "FROM FACT0 AS F0, DATA_TBL AS D " +
                "WHERE D.FACT0_ID=F0.FACT_ID " +
                "AND D.NO_IDX_ID >=? AND D.NO_IDX_ID < ?");
        qrys.put("SQL_NL_2_TBLS_BY_LONG_JO1_FILTERED_BY_NOIDX",
            "SELECT D.VAL, F0.VAL " +
                "FROM DATA_TBL AS D, FACT0 AS F0 " +
                "WHERE D.FACT0_ID=F0.FACT_ID " +
                "AND D.NO_IDX_ID >=? AND D.NO_IDX_ID < ?");

        qrys.put("SQL_NL_2_TBLS_BY_STR_JO0_FILTERED_BY_IDX",
            "SELECT D.VAL, F1.VAL " +
                "FROM FACT1 AS F1, DATA_TBL AS D " +
                "WHERE D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.IDX_ID >=? AND D.IDX_ID < ?");
        qrys.put("SQL_NL_2_TBLS_BY_STR_JO1_FILTERED_BY_IDX",
            "SELECT D.VAL, F1.VAL " +
                "FROM DATA_TBL AS D, FACT1 AS F1 " +
                "WHERE D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.IDX_ID >=? AND D.IDX_ID < ?");

        qrys.put("SQL_NL_2_TBLS_BY_STR_JO0_FILTERED_BY_NOIDX",
            "SELECT D.VAL, F1.VAL " +
                "FROM FACT1 AS F1, DATA_TBL AS D " +
                "WHERE D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.NO_IDX_ID >=? AND D.NO_IDX_ID < ?");
        qrys.put("SQL_NL_2_TBLS_BY_STR_JO1_FILTERED_BY_NOIDX",
            "SELECT D.VAL, F1.VAL " +
                "FROM DATA_TBL AS D, FACT1 AS F1 " +
                "WHERE D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.NO_IDX_ID >=? AND D.NO_IDX_ID < ?");

        // 2 tables filtered, HASH INDEX
        qrys.put("SQL_HJ_2_TBLS_BY_LONG_FILTERED_BY_IDX",
            "SELECT D.VAL, F0.VAL " +
                "FROM DATA_TBL AS D, FACT0 AS F0 USE INDEX (HASH_JOIN_IDX) " +
                "WHERE D.FACT0_ID=F0.FACT_ID " +
                "AND D.IDX_ID >=? AND D.IDX_ID < ?");

        qrys.put("SQL_HJ_2_TBLS_BY_LONG_FILTERED_BY_NOIDX",
            "SELECT D.VAL, F0.VAL " +
                "FROM DATA_TBL AS D, FACT0 AS F0 USE INDEX (HASH_JOIN_IDX) " +
                "WHERE D.FACT0_ID=F0.FACT_ID " +
                "AND D.NO_IDX_ID >=? AND D.NO_IDX_ID < ?");

        qrys.put("SQL_HJ_2_TBLS_BY_LONG_CONVERTED_FILTERED_BY_IDX",
            "SELECT D.VAL, F0.VAL " +
                "FROM DATA_TBL AS D, FACT0_INT AS F0 USE INDEX (HASH_JOIN_IDX) " +
                "WHERE D.FACT0_ID=F0.FACT_ID " +
                "AND D.IDX_ID >=? AND D.IDX_ID < ?");

        qrys.put("SQL_HJ_2_TBLS_BY_LONG_CONVERTED_FILTERED_BY_NOIDX",
            "SELECT D.VAL, F0.VAL " +
                "FROM DATA_TBL AS D, FACT0_INT AS F0 USE INDEX (HASH_JOIN_IDX) " +
                "WHERE D.FACT0_ID=F0.FACT_ID " +
                "AND D.NO_IDX_ID >=? AND D.NO_IDX_ID < ?");

        qrys.put("SQL_HJ_2_TBLS_BY_STR_FILTERED_BY_IDX",
            "SELECT D.VAL, F1.VAL " +
                "FROM DATA_TBL AS D, FACT1 AS F1 USE INDEX (HASH_JOIN_IDX) " +
                "WHERE D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.IDX_ID >=? AND D.IDX_ID < ?");

        qrys.put("SQL_HJ_2_TBLS_BY_STR_FILTERED_BY_NOIDX",
            "SELECT D.VAL, F1.VAL " +
                "FROM DATA_TBL AS D, FACT1 AS F1 USE INDEX (HASH_JOIN_IDX) " +
                "WHERE D.FACT1_ID_STR=F1.FACT_ID_STR " +
                "AND D.NO_IDX_ID >=? AND D.NO_IDX_ID < ?");
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        String qryName = args.getStringParameter("qryName", "SQL_HJ_3_TBLS");

        sql = qrys.get(qryName);

        if (sql == null) {
            throw new Exception("Invalid query name: " + qryName
                + ". Available queries: " + qrys.keySet());
        }

        firstParam = args.getBooleanParameter("firstParam", false);
        secondParam = args.getBooleanParameter("secondParam", false);
        firstParamRange = args.getLongParameter("firstParamRange", Long.MAX_VALUE);
        range = args.getLongParameter("range", 0);
        initStep = args.getBooleanParameter("init", false);
        createIndexStep = args.getBooleanParameter("createIndex", false);

        printParameters();

        if (initStep)
            init();

        if (createIndexStep)
            createIndex();

        printPlan();
    }

    /**
     *
     */
    private void init() {
        IgniteSemaphore sem = ignite().semaphore("sql-setup", 1, true, true);

        try {
            if (sem.tryAcquire()) {
                println(cfg, "Create tables...");

                sql("CREATE TABLE DATA_TBL(" +
                    "ID LONG PRIMARY KEY, " +
                    "FACT0_ID LONG, " +
                    "FACT1_ID_STR VARCHAR, " +
                    "VAL VARCHAR, " +
                    "NO_IDX_ID LONG," +
                    "IDX_ID LONG) " +
                    "WITH \"CACHE_NAME=DATA_TBL,VALUE_TYPE=DATA_VAL\"");

                sql("CREATE TABLE FACT0(" +
                    "ID LONG PRIMARY KEY, " +
                    "FACT_ID LONG, " +
                    "VAL VARCHAR) " +
                    "WITH \"TEMPLATE=REPLICATED,CACHE_NAME=FACT0,VALUE_TYPE=FACT0_VAL\"");

                sql("CREATE TABLE FACT0_INT(" +
                    "ID LONG PRIMARY KEY, " +
                    "FACT_ID INT, " +
                    "VAL VARCHAR) " +
                    "WITH \"TEMPLATE=REPLICATED,CACHE_NAME=FACT0_INT,VALUE_TYPE=FACT0_INT_VAL\"");

                sql("CREATE TABLE FACT1(" +
                    "ID LONG PRIMARY KEY, " +
                    "FACT_ID_STR VARCHAR, " +
                    "VAL VARCHAR) " +
                    "WITH \"TEMPLATE=REPLICATED,CACHE_NAME=FACT1,VALUE_TYPE=FACT1_VAL\"");

                sql("CREATE INDEX DATA_TBL_IDX_ID_IDX ON DATA_TBL(IDX_ID)");

                final int fact0Size = args.getIntParameter("FACTS0_SIZE", 1000);
                final int fact1Size = args.getIntParameter("FACTS1_SIZE", 1000);

                fillCache("DATA_TBL", "DATA_VAL", (long)args.range(),
                    (k, bob) -> {
                        bob.setField("FACT0_ID", ThreadLocalRandom.current().nextLong(fact0Size));
                        bob.setField("FACT1_ID_STR",
                            fact1IdByKey(ThreadLocalRandom.current().nextLong(fact1Size)));
                        bob.setField("VAL", UUID.randomUUID());
                        bob.setField("NO_IDX_ID", k);
                        bob.setField("IDX_ID", k);
                    });

                println(cfg, "Populate FACT0: " + fact0Size);
                fillCache("FACT0", "FACT0_VAL", fact0Size,
                    (k, bob) -> {
                        bob.setField("FACT_ID", k);
                        bob.setField("VAL", "FACT0_" + k);
                    });

                println(cfg, "Populate FACT0_INT: " + fact0Size);
                fillCache("FACT0_INT", "FACT0_INT_VAL", fact0Size,
                    (k, bob) -> {
                        bob.setField("FACT_ID", k.intValue());
                        bob.setField("VAL", "FACT0_" + k);
                    });

                println(cfg, "Populate FACT1: " + fact1Size);
                fillCache("FACT1", "FACT1_VAL", fact0Size,
                    (k, bob) -> {
                        bob.setField("FACT_ID_STR", fact1IdByKey(k));
                        bob.setField("VAL", "FACT1_" + k);
                    });

                println(cfg, "Finished populating data");
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
    }

    /**
     * Create index for JOIN.
     */
    private void createIndex() {
        println("Create indexes...");
        sql("CREATE INDEX DATA_TBL_FACT0_FACT1_IDX ON DATA_TBL(FACT0_ID, FACT1_ID_STR)");
        sql("CREATE INDEX DATA_TBL_FACT0_IDX ON DATA_TBL(FACT0_ID)");
        sql("CREATE INDEX DATA_TBL_FACT1_IDX ON DATA_TBL(FACT1_ID_STR)");
        sql("CREATE INDEX FACT0_FACT_ID_IDX ON FACT0(FACT_ID)");
        sql("CREATE INDEX FACT1_FACT_STR_ID_IDX ON FACT1(FACT_ID_STR)");
    }

    /**
     * @param name Cache name.
     * @param typeName Type name.
     * @param range Key range.
     * @param fillDataConsumer Fill one object by key.
     */
    @SuppressWarnings("unchecked")
    private void fillCache(
        String name,
        String typeName,
        long range,
        BiConsumer<Long, BinaryObjectBuilder> fillDataConsumer) {
        println(cfg, "Populate cache: " + name + ", range: " + range);

        try (IgniteDataStreamer stream = ignite().dataStreamer(name)) {
            stream.allowOverwrite(false);

            for (long k = 0; k < range; ++k) {
                BinaryObjectBuilder bob = ignite().binary().builder(typeName);

                fillDataConsumer.accept(k, bob);

                stream.addData(k, bob.build());
            }
        }
    }

    /**
     * @param key ID key.
     * @return Fact1 string ID.
     */
    private String fact1IdByKey(long key) {
        return String.format("The long static prefix." +
            "------------------------------------------------------------------------ %06d", key);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        FieldsQueryCursor<List<?>> cur;
        if (!firstParam)
            cur = sql(sql);
        else if (!secondParam)
            cur = sql(sql, ThreadLocalRandom.current().nextLong(firstParamRange));
        else {
            long p0 = ThreadLocalRandom.current().nextLong(firstParamRange);

            cur = sql(sql, p0, p0 + range);
        }

        Iterator it = cur.iterator();

        long cnt = 0;

        while (it.hasNext()) {
            it.next();
            ++cnt;
        }

        return true;
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

    /**
     *
     */
    private void printParameters() {
        println("Benchmark parameter:");
        println("    SQL: " + sql);
        println("    init: " + initStep);
        println("    create index: " + createIndexStep);
        println("    1st param: " + firstParam);
        println("    1nd param range: " + firstParamRange);
        println("    2nd param: " + secondParam);
        println("    2nd param range: " + range);
    }

    /**
     *
     */
    private void printPlan() {
        FieldsQueryCursor<List<?>> planCur;

        if (!firstParam)
            planCur = sql("EXPLAIN " + sql);
        else if (!secondParam)
            planCur = sql("EXPLAIN " + sql, ThreadLocalRandom.current().nextLong(firstParamRange));
        else {
            long p0 = ThreadLocalRandom.current().nextLong(firstParamRange);

            planCur = sql("EXPLAIN " + sql, p0, p0 + range);
        }

        println("Plan: " + planCur.getAll().toString());
    }
}
