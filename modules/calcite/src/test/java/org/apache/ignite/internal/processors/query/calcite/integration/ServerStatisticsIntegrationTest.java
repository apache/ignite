/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsManager;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for server side statistics usage.
 */
public class ServerStatisticsIntegrationTest extends AbstractBasicIntegrationTest {
    /** Server instance. */
    private IgniteEx srv;

    /** All types table nullable fields. */
    private static final String[] NULLABLE_FIELDS = {//"string_field",
        //"byte_arr_field",
        "boolean_obj_field",
        "short_obj_field",
        "integer_field",
        "long_obj_field",
        "float_obj_field",
        "double_obj_field",
        //"date_field",
        //"time_field",
        //"timestamp_field"
    };

    /** All types table non nullable fields. */
    String[] NON_NULLABLE_FIELDS = {
        //"boolean_field", // TODO: 25 row?
        "short_field",
        "int_field",
        "long_field",
        "float_field",
        "double_field"
    };

    /** All types table numeric fields. */
    private static final String[] NUMERIC_FIELDS = {
        "short_obj_field",
        "integer_field",
        "long_obj_field",
        "float_obj_field",
        "double_obj_field",
        "short_field",
        "int_field",
        "long_field",
        "float_field",
        "double_field"
    };

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        createAndPopulateAllTypesTable(0, 100);
    }

    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        cleanQueryPlanCache();
    }

    /**
     * Run select and check that cost take statisitcs in account:
     * 1) without statistics;
     * 2) with statistics;
     * 3) after deleting statistics.
     */
    @Test
    public void testQueryCostWithStatistics() throws IgniteCheckedException {
        createAndPopulateTable();
        StatisticsKey key = new StatisticsKey("PUBLIC", "PERSON");
        srv = ignite(0);

        TestCost costWoStats = new TestCost(1000., 1000., null, null, null);

        assertQuerySrv("select count(name) from person").matches(QueryChecker.containsCost(costWoStats)).check();

        clearQryCache(srv);

        collectStatistics(srv, key);

        TestCost costWithStats = new TestCost(5., 5., null, null, null);

        assertQuerySrv("select count(name) from person").matches(QueryChecker.containsCost(costWithStats)).check();

        statMgr(srv).dropStatistics(new StatisticsTarget(key));
        clearQryCache(srv);

        assertQuerySrv("select count(name) from person").matches(QueryChecker.containsCost(costWoStats)).check();
    }

    @Test
    public void testAllTypesCost() throws IgniteCheckedException, InterruptedException {
        StatisticsKey key = new StatisticsKey("PUBLIC", "ALL_TYPES");
        srv = ignite(0);

        TestCost costWoStats = new TestCost(1000., 1000., null, null, null);

        collectStatistics(srv, key);

        testNullableFields();

        testBorders();

        statMgr(srv).dropStatistics(new StatisticsTarget(key));
        clearQryCache(srv);

        assertQuerySrv("select * from all_types").matches(QueryChecker.containsCost(costWoStats)).check();
    }

    /**
     * Check is null conditions.
     */
    public void testNullableFields() {
        String sql = "select * from all_types ";

        for (String nullableField : NULLABLE_FIELDS) {
            assertQuerySrv(sql + "where " + nullableField + " is null").matches(QueryChecker.containsRowCount(25.)).check();

            assertQuerySrv(sql + "where " + nullableField + " is not null").matches(QueryChecker.containsRowCount(75.)).check();
        }

        for (String nullableField : NULLABLE_FIELDS) {
            assertQuerySrv(sql + "where " + nullableField + " is null").matches(QueryChecker.containsRowCount(25.)).check();

            assertQuerySrv(sql + "where " + nullableField + " is not null").matches(QueryChecker.containsRowCount(75.)).check();
        }
    }

    /**
     * Check randge with min/max borders.
     */
    private void testBorders() {
        String sql = "select * from all_types ";

        Set<String> nonNullableFields = new HashSet(Arrays.asList(NON_NULLABLE_FIELDS));
        for (String numericField : NUMERIC_FIELDS) {
            double rowCount = (nonNullableFields.contains(numericField)) ? 100. : 75.;
            System.out.println(numericField);
            // TODO: why 1
            assertQuerySrv(sql + "where " + numericField + " <  -1").matches(QueryChecker.containsRowCount(1.)).check();
            assertQuerySrv(sql + "where " + numericField + " <  0").matches(QueryChecker.containsRowCount(1.)).check();
            assertQuerySrv(sql + "where " + numericField + " <=  0").matches(QueryChecker.containsRowCount(1.)).check();
            assertQuerySrv(sql + "where " + numericField + " >=  0").matches(QueryChecker.containsRowCount(rowCount)).check();
            assertQuerySrv(sql + "where " + numericField + " > 0").matches(QueryChecker.containsRowCount(rowCount)).check();
        }
    }

    /**
     * Clear query cache in specified node.
     *
     * @param ign Ignite node to clear calcite query cache on.
     */
    protected void clearQryCache(IgniteEx ign) {
        CalciteQueryProcessor qryProc = (CalciteQueryProcessor)Commons.lookupComponent(
            (ign).context(), QueryEngine.class);

        qryProc.queryPlanCache().clear();
    }

    /**
     * Collect statistics by speicifed key on specified node.
     *
     * @param ign Node to collect statistics on.
     * @param key Statistics key to collect statistics by.
     * @throws IgniteCheckedException In case of errors.
     */
    protected void collectStatistics(IgniteEx ign, StatisticsKey key) throws IgniteCheckedException {
        IgniteStatisticsManager statMgr = statMgr(ign);

        statMgr.collectStatistics(new StatisticsObjectConfiguration(key));

        assertTrue(GridTestUtils.waitForCondition(() -> statMgr.getLocalStatistics(key) != null, 1000));
    }

    /**
     * Get statistics manager.
     *
     * @param ign Node to get statistics manager from.
     * @return IgniteStatisticsManager.
     */
    protected IgniteStatisticsManager statMgr(IgniteEx ign) {
        IgniteH2Indexing indexing = (IgniteH2Indexing)ign.context().query().getIndexing();

        return indexing.statsManager();
    }

    /** */
    protected QueryChecker assertQuerySrv(String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryEngine getEngine() {
                return Commons.lookupComponent(srv.context(), QueryEngine.class);
            }
        };
    }

    /**
     * Create (if not exists) and populate cache with all types.
     *
     * @param start
     * @param count
     * @return
     */
    protected IgniteCache<Integer, AllTypes> createAndPopulateAllTypesTable(int start, int count) {
        IgniteCache<Integer, AllTypes> all_types = grid(0).getOrCreateCache(new CacheConfiguration<Integer, AllTypes>()
            .setName("all_types")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, AllTypes.class).setTableName("all_types")))
            .setBackups(2)
        );

        for (int i = start; i < start + count; i++) {
            boolean null_values = (i & 3) == 1;

            all_types.put(i, new AllTypes(i, null_values));
        }

        return all_types;
    }

    /**
     * Test cost with nulls for unknown values.
     */
    public static class TestCost {
        /** */
        Double rowCount;

        /** */
        Double cpu;

        /** */
        Double memory;

        /** */
        Double io;

        /** */
        Double network;

        /**
         * @return Row count.
         */
        public Double rowCount() {
            return rowCount;
        }

        /**
         * @return Cpu.
         */
        public Double cpu() {
            return cpu;
        }

        /**
         * @return Memory
         */
        public Double memory() {
            return memory;
        }

        /**
         * @return Io.
         */
        public Double io() {
            return io;
        }

        /**
         * @return Network.
         */
        public Double network() {
            return network;
        }

        /**
         * Constructor.
         *
         * @param rowCount Row count.
         * @param cpu Cpu.
         * @param memory Memory.
         * @param io Io.
         * @param network Network.
         */
        public TestCost(Double rowCount, Double cpu, Double memory, Double io, Double network) {
            this.rowCount = rowCount;
            this.cpu = cpu;
            this.memory = memory;
            this.io = io;
            this.network = network;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestCost{" +
                "rowCount=" + rowCount +
                ", cpu=" + cpu +
                ", memory=" + memory +
                ", io=" + io +
                ", network=" + network +
                '}';
        }
    }

    /**
     * Test class with fields of all types.
     */
    public static class AllTypes {
        /** */
        @QuerySqlField
        public String string_field;

        /** */
        @QuerySqlField
        public byte[] byte_arr_field;

        /** */
        @QuerySqlField
        public boolean boolean_field;

        /** */
        @QuerySqlField
        public Boolean boolean_obj_field;

//        /** */
//        @QuerySqlField
//        public char char_field;
//
//        /** */
//        @QuerySqlField
//        public Character character_field;

        /** */
        @QuerySqlField
        public short short_field;

        /** */
        @QuerySqlField
        public Short short_obj_field;

        /** */
        @QuerySqlField
        public int int_field;

        /** */
        @QuerySqlField
        public Integer integer_field;

        /** */
        @QuerySqlField
        public long long_field;

        /** */
        @QuerySqlField
        public Long long_obj_field;

        /** */
        @QuerySqlField
        public float float_field;

        /** */
        @QuerySqlField
        public Float float_obj_field;

        /** */
        @QuerySqlField
        public double double_field;

        /** */
        @QuerySqlField
        public Double double_obj_field;

        /** */
        @QuerySqlField
        public Date date_field;

        /** */
        @QuerySqlField
        public Time time_field;

        /** */
        @QuerySqlField
        public Timestamp timestamp_field;

        public AllTypes(int i, boolean null_val) {
            string_field = (null_val) ? null : "string_field_value" + i;
            byte_arr_field = (null_val) ? null : BigInteger.valueOf(i).toByteArray();
            boolean_field = (i & 1) == 0;
            boolean_obj_field = (null_val) ? null : (i & 1) == 0;
            // char_field = (char)((i % 20) + (int)('a'));
            // character_field = (null_val) ? null : char_field;
            short_field = (short)i;
            short_obj_field = (null_val) ? null : short_field;
            int_field = i;
            integer_field = (null_val) ? null : i;
            long_field = i;
            long_obj_field = (null_val) ? null : long_field;
            float_field = i;
            float_obj_field = (null_val) ? null : float_field;
            double_field = i;
            double_obj_field = (null_val) ? null : double_field;
            date_field = (null_val) ? null : Date.valueOf(String.format("%04d-04-09", i));
            time_field = (null_val) ? null : new Time(i);
            timestamp_field = (null_val) ? null : new Timestamp(i);
        }

        public AllTypes(String string_field, byte[] byte_arr_field, boolean boolean_field, Boolean boolean_obj_field,
            char char_field, Character character_field, short short_field, Short short_obj_field, int int_field,
            Integer integer_field, long long_field, Long long_obj_field, float float_field, Float float_obj_field,
            double double_field, Double double_obj_field, Date date_field, Time time_field, Timestamp timestamp_field) {
            this.string_field = string_field;
            this.byte_arr_field = byte_arr_field;
            this.boolean_field = boolean_field;
            this.boolean_obj_field = boolean_obj_field;
//            this.char_field = char_field;
//            this.character_field = character_field;
            this.short_field = short_field;
            this.short_obj_field = short_obj_field;
            this.int_field = int_field;
            this.integer_field = integer_field;
            this.long_field = long_field;
            this.long_obj_field = long_obj_field;
            this.float_field = float_field;
            this.float_obj_field = float_obj_field;
            this.double_field = double_field;
            this.double_obj_field = double_obj_field;
            this.date_field = date_field;
            this.time_field = time_field;
            this.timestamp_field = timestamp_field;
        }

        @Override public String toString() {
            return "AllTypes{" +
                "string_field='" + string_field + '\'' +
                ", byte_arr_field=" + byte_arr_field +
                ", boolean_field=" + boolean_field +
                ", boolean_obj_field=" + boolean_obj_field +
//                ", char_field=" + char_field +
//                ", character_field=" + character_field +
                ", short_field=" + short_field +
                ", short_obj_field=" + short_obj_field +
                ", int_field=" + int_field +
                ", Integer_field=" + integer_field +
                ", long_field=" + long_field +
                ", long_obj_field=" + long_obj_field +
                ", float_field=" + float_field +
                ", float_obj_field=" + float_obj_field +
                ", double_field=" + double_field +
                ", double_obj_field=" + double_obj_field +
                ", date_field=" + date_field +
                ", time_field=" + time_field +
                ", timestamp_field=" + timestamp_field +
                '}';
        }
    }
}
