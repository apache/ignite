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
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for server side statistics usage.
 */
public class ServerStatisticsIntegrationTest extends AbstractBasicIntegrationTest {
    /** Server instance. */
    private IgniteEx srv;

    /** All types table row count. */
    private static final int ROW_COUNT = 100;

    /** All types table nullable fields. */
    private static final String[] NULLABLE_FIELDS = {
        "string_field",
        "boolean_obj_field",
        "short_obj_field",
        "integer_field",
        "long_obj_field",
        "float_obj_field",
        "double_obj_field",
    };

    /** All types table non nullable fields. */
    private static final String[] NON_NULLABLE_FIELDS = {
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

        createAndPopulateAllTypesTable(0, ROW_COUNT);
    }

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        cleanQueryPlanCache();
    }

    /**
     * Run select and check that result rows take statisitcs in account:
     * 1) without statistics - by row count and heuristic;
     * 2) with statistics - by statistics;
     * 3) after deleting statistics - by row count and heuristics again.
     */
    @Test
    public void testQueryCostWithStatistics() throws IgniteCheckedException {
        String sql = "select name from person where salary is not null";

        createAndPopulateTable();

        StatisticsKey key = new StatisticsKey("PUBLIC", "PERSON");
        srv = ignite(0);

        assertQuerySrv(sql).matches(QueryChecker.containsResultRowCount(4.5)).check();

        clearQryCache(srv);

        collectStatistics(key);

        assertQuerySrv(sql).matches(QueryChecker.containsResultRowCount(5)).check();

        dropStatistics(key);
        clearQryCache(srv);

        assertQuerySrv(sql).matches(QueryChecker.containsResultRowCount(4.5)).check();
    }

    /**
     * Check is null/is not null conditions for nullable and non nullable fields.
     */
    @Test
    public void testNullConditions() throws IgniteCheckedException {
        StatisticsKey key = new StatisticsKey("PUBLIC", "ALL_TYPES");
        srv = ignite(0);

        collectStatistics(key);

        String sql = "select * from all_types ";

        for (String nullableField : NULLABLE_FIELDS) {
            assertQuerySrv(sql + "where " + nullableField + " is null")
                .matches(QueryChecker.containsResultRowCount(25.)).check();

            assertQuerySrv(sql + "where " + nullableField + " is not null")
                .matches(QueryChecker.containsResultRowCount(75.)).check();
        }

        for (String nonNullableField : NON_NULLABLE_FIELDS) {
            assertQuerySrv(sql + "where " + nonNullableField + " is null")
                .matches(QueryChecker.containsResultRowCount(1.)).check();

            assertQuerySrv(sql + "where " + nonNullableField + " is not null")
                .matches(QueryChecker.containsResultRowCount(ROW_COUNT)).check();
        }
    }

    /**
     * Test multiple condition for the same query.
     *
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void testMultipleConditionQuery() throws IgniteCheckedException {
        StatisticsKey key = new StatisticsKey("PUBLIC", "ALL_TYPES");
        srv = ignite(0);

        collectStatistics(key);

        Set<String> nonNullableFields = new HashSet<>(Arrays.asList(NON_NULLABLE_FIELDS));

        for (String numericField : NUMERIC_FIELDS) {
            double allRowCnt = (nonNullableFields.contains(numericField)) ? (double)ROW_COUNT : 0.75 * ROW_COUNT;

            String fieldSql = String.format("select * from all_types where %s > -100 and %s > 0", numericField,
                numericField);

            assertQuerySrv(fieldSql).matches(QueryChecker.containsResultRowCount(allRowCnt)).check();

            fieldSql = String.format("select * from all_types where %s < 1000 and %s < 101", numericField,
                numericField);

            assertQuerySrv(fieldSql).matches(QueryChecker.containsResultRowCount(allRowCnt)).check();

            fieldSql = String.format("select * from all_types where %s > -100 and %s < 1000", numericField,
                numericField);

            assertQuerySrv(fieldSql).matches(QueryChecker.containsResultRowCount(allRowCnt)).check();
        }
    }

    /**
     * Check range condition with not null conditions.
     *
     * @throws IgniteCheckedException In case of error.
     */
    @Test
    public void testNonNullMultipleConditionQuery() throws IgniteCheckedException {
        StatisticsKey key = new StatisticsKey("PUBLIC", "ALL_TYPES");
        srv = ignite(0);

        collectStatistics(key);

        Set<String> nonNullableFields = new HashSet<>(Arrays.asList(NON_NULLABLE_FIELDS));

        // time
        String timeSql = "select * from all_types where time_field is not null";

        assertQuerySrv(timeSql).matches(QueryChecker.containsResultRowCount(ROW_COUNT * 0.75)).check();

        timeSql += " and time_field > '00:00:00'";

        assertQuerySrv(timeSql).matches(QueryChecker.containsResultRowCount(ROW_COUNT * 0.75)).check();

        // date
        String dateSql = "select * from all_types where date_field is not null";

        assertQuerySrv(dateSql).matches(QueryChecker.containsResultRowCount(ROW_COUNT * 0.75)).check();

        dateSql += " and date_field > '1000-01-01'";

        assertQuerySrv(dateSql).matches(QueryChecker.containsResultRowCount(ROW_COUNT * 0.75)).check();

        // timestamp
        String timestampSql = "select * from all_types where timestamp_field is not null ";

        assertQuerySrv(timestampSql).matches(QueryChecker.containsResultRowCount(ROW_COUNT * 0.75)).check();

        timestampSql += " and timestamp_field > '1000-01-10 11:59:59'";

        assertQuerySrv(timestampSql).matches(QueryChecker.containsResultRowCount(ROW_COUNT * 0.75)).check();

        // numeric fields
        for (String numericField : NUMERIC_FIELDS) {
            double allRowCnt = (nonNullableFields.contains(numericField)) ? (double)ROW_COUNT : 0.75 * ROW_COUNT;

            String fieldSql = String.format("select * from all_types where %s is not null", numericField);

            assertQuerySrv(fieldSql).matches(QueryChecker.containsResultRowCount(allRowCnt)).check();

            fieldSql = String.format("select * from all_types where %s is not null and %s > 0", numericField,
                numericField);

            assertQuerySrv(fieldSql).matches(QueryChecker.containsResultRowCount(allRowCnt)).check();
        }
    }

    /**
     * Check condition with projections:
     *
     * 1) Condition on the one of fields in select list.
     * 2) Confition on the field not from select list.
     *
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void testProjections() throws IgniteCheckedException {
        StatisticsKey key = new StatisticsKey("PUBLIC", "ALL_TYPES");
        srv = ignite(0);

        collectStatistics(key);

        String sql = "select %s, %s from all_types where %s < " + ROW_COUNT;

        String sql2 = "select %s from all_types where %s >= " + (-ROW_COUNT);

        Set<String> nonNullableFields = new HashSet<>(Arrays.asList(NON_NULLABLE_FIELDS));

        for (int firstFieldIdx = 0; firstFieldIdx < NUMERIC_FIELDS.length - 1; firstFieldIdx++) {
            String firstField = NUMERIC_FIELDS[firstFieldIdx];
            double firstAllRowCnt = (nonNullableFields.contains(firstField)) ? (double)ROW_COUNT : 0.75 * ROW_COUNT;

            for (int secFieldIdx = firstFieldIdx + 1; secFieldIdx < NUMERIC_FIELDS.length; secFieldIdx++) {
                String secField = NUMERIC_FIELDS[secFieldIdx];

                double secAllRowCnt = (nonNullableFields.contains(secField)) ? (double)ROW_COUNT : 0.75 * ROW_COUNT;

                String qry = String.format(sql, secField, firstField, secField);

                assertQuerySrv(qry).matches(QueryChecker.containsResultRowCount(secAllRowCnt)).check();

                qry = String.format(sql, firstField, secField, firstField);

                assertQuerySrv(qry).matches(QueryChecker.containsResultRowCount(firstAllRowCnt)).check();

                qry = String.format(sql2, firstField, secField);

                assertQuerySrv(qry).matches(QueryChecker.containsResultRowCount(secAllRowCnt)).check();

                qry = String.format(sql2, secField, firstField);

                assertQuerySrv(qry).matches(QueryChecker.containsResultRowCount(firstAllRowCnt)).check();
            }
        }
    }

    /**
     * Test not null counting with two range conjuncted condition on one and two columns.
     *
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void testNotNullCountingSelectivity() throws IgniteCheckedException {
        StatisticsKey key = new StatisticsKey("PUBLIC", "ALL_TYPES");
        srv = ignite(0);

        collectStatistics(key);

        Set<String> nonNullableFields = new HashSet<>(Arrays.asList(NON_NULLABLE_FIELDS));

        for (String numericField : NUMERIC_FIELDS) {
            double allRowCnt = (nonNullableFields.contains(numericField)) ? (double)ROW_COUNT : 0.75 * ROW_COUNT;

            assertQuerySrv(String.format("select * from all_types where " +
                "%s > %d and %s < %d", numericField, -1, numericField, 101))
                .matches(QueryChecker.containsResultRowCount(allRowCnt)).check();

            assertQuerySrv(String.format("select /*+ DISABLE_RULE('ScanLogicalOrToUnionRule') */ * from all_types where " +
                "(%s > %d and %s < %d) or " +
                "(int_field > -1 and int_field < 101)",
                numericField, -1, numericField, 101))
                .matches(QueryChecker.containsResultRowCount(ROW_COUNT)).check();
        }
    }

    /**
     * Test disjunctions selectivity for each column:
     * 1) with select all conditions
     * 2) with select none conditions
     * 3) with is null or select all conditions
     * 4) with is null or select none conditions
     *
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void testDisjunctionSelectivity() throws IgniteCheckedException {
        StatisticsKey key = new StatisticsKey("PUBLIC", "ALL_TYPES");
        srv = ignite(0);

        collectStatistics(key);

        Set<String> nonNullableFields = new HashSet<>(Arrays.asList(NON_NULLABLE_FIELDS));

        for (String numericField : NUMERIC_FIELDS) {
            double allRowIsNullCnt = (nonNullableFields.contains(numericField)) ? (double)ROW_COUNT : 0.8125 * ROW_COUNT;
            double allRowRangeCnt = (nonNullableFields.contains(numericField)) ? (double)ROW_COUNT : 0.75 * ROW_COUNT;

            assertQuerySrv(String.format("select * from all_types where " +
                "%s > %d or %s < %d", numericField, -1, numericField, 101))
                .matches(QueryChecker.containsResultRowCount(allRowRangeCnt)).check();

            assertQuerySrv(String.format("select * from all_types where " +
                "%s > %d or %s < %d", numericField, 101, numericField, -1))
                .matches(QueryChecker.containsResultRowCount(1.)).check();

            assertQuerySrv(String.format("select * from all_types where " +
                "%s > %d or %s is null", numericField, -1, numericField))
                .matches(QueryChecker.containsResultRowCount(allRowIsNullCnt)).check();

            assertQuerySrv(String.format("select * from all_types where " +
                "%s > %d or %s is null", numericField, 101, numericField))
                .matches(QueryChecker.containsResultRowCount(
                    nonNullableFields.contains(numericField) ? 1. : (double)ROW_COUNT * 0.25)).check();
        }
    }

    /**
     * Check randge with min/max borders.
     */
    @Test
    public void testBorders() throws IgniteCheckedException {
        StatisticsKey key = new StatisticsKey("PUBLIC", "ALL_TYPES");
        srv = ignite(0);

        collectStatistics(key);

        // time
        String timeSql = "select * from all_types where time_field > '00:00:00'";

        assertQuerySrv(timeSql).matches(QueryChecker.containsResultRowCount(ROW_COUNT * 0.75)).check();

        // date
        String dateSql = "select * from all_types where date_field > '1000-01-10'";

        assertQuerySrv(dateSql).matches(QueryChecker.containsResultRowCount(ROW_COUNT * 0.75)).check();

        // timestamp
        String timestampSql = "select * from all_types where timestamp_field > '1000-01-10 11:59:59'";

        assertQuerySrv(timestampSql).matches(QueryChecker.containsResultRowCount(ROW_COUNT * 0.75)).check();

        String sql = "select * from all_types ";

        Set<String> nonNullableFields = new HashSet<>(Arrays.asList(NON_NULLABLE_FIELDS));
        for (String numericField : NUMERIC_FIELDS) {
            double allRowCnt = (nonNullableFields.contains(numericField)) ? (double)ROW_COUNT : 0.75 * ROW_COUNT;

            String fieldSql = sql + "where " + numericField;

            assertQuerySrv(fieldSql + " <  -1").matches(QueryChecker.containsResultRowCount(1.)).check();
            assertQuerySrv(fieldSql + " <  0").matches(QueryChecker.containsResultRowCount(1.)).check();
            assertQuerySrv(fieldSql + " <=  0").matches(QueryChecker.containsResultRowCount(1.)).check();
            assertQuerySrv(fieldSql + " >=  0").matches(QueryChecker.containsResultRowCount(allRowCnt)).check();
            assertQuerySrv(fieldSql + " > 0").matches(QueryChecker.containsResultRowCount(allRowCnt)).check();

            assertQuerySrv(fieldSql + " > 101").matches(QueryChecker.containsResultRowCount(1.)).check();
            assertQuerySrv(fieldSql + " > 100").matches(QueryChecker.containsResultRowCount(1.)).check();
            assertQuerySrv(fieldSql + " >= 100").matches(QueryChecker.containsResultRowCount(1.)).check();
            assertQuerySrv(fieldSql + " <= 100").matches(QueryChecker.containsResultRowCount(allRowCnt)).check();
            assertQuerySrv(fieldSql + " < 100").matches(QueryChecker.containsResultRowCount(allRowCnt)).check();
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
     * @param key Statistics key to collect statistics by.
     * @throws IgniteCheckedException In case of errors.
     */
    protected void collectStatistics(StatisticsKey key) throws IgniteCheckedException {
        executeSql(String.format("ANALYZE %s.%s", key.schema(), key.obj()));

        assertTrue(
            GridTestUtils.waitForCondition(
                () -> !F.isEmpty(sql(srv, "select * from sys.statistics_local_data where name = ?", key.obj())),
                1000
            )
        );
    }

    /**
     * Drop statistics for specified key.
     *
     * @param key Statistics key to collect statistics for.
     */
    protected void dropStatistics(StatisticsKey key) {
        executeSql(String.format("DROP STATISTICS %s.%s", key.schema(), key.obj()));
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
     * @param start First key idx.
     * @param count Rows count.
     * @return Populated cache.
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
     * Test class with fields of all types.
     */
    public static class AllTypes {
        /** */
        @QuerySqlField(name = "string_field")
        public String stringField;

        /** */
        @QuerySqlField(name = "byte_arr_field")
        public byte[] byteArrField;

        /** */
        @QuerySqlField(name = "boolean_field")
        public boolean booleanField;

        /** */
        @QuerySqlField(name = "boolean_obj_field")
        public Boolean booleanObjField;

        /** */
        @QuerySqlField(name = "short_field")
        public short shortField;

        /** */
        @QuerySqlField(name = "short_obj_field")
        public Short shortObjField;

        /** */
        @QuerySqlField(name = "int_field")
        public int intField;

        /** */
        @QuerySqlField(name = "integer_field")
        public Integer integerField;

        /** */
        @QuerySqlField(name = "long_field")
        public long longField;

        /** */
        @QuerySqlField(name = "long_obj_field")
        public Long longObjField;

        /** */
        @QuerySqlField(name = "float_field")
        public float floatField;

        /** */
        @QuerySqlField(name = "float_obj_field")
        public Float floatObjField;

        /** */
        @QuerySqlField(name = "double_field")
        public double doubleField;

        /** */
        @QuerySqlField(name = "double_obj_field")
        public Double doubleObjField;

        /** */
        @QuerySqlField(name = "date_field")
        public Date dateField;

        /** */
        @QuerySqlField(name = "time_field")
        public Time timeField;

        /** */
        @QuerySqlField(name = "timestamp_field")
        public Timestamp timestampField;

        /**
         * Constructor.
         *
         * @param i idx to generate all fields values by.
         * @param null_val Should object fields be equal to {@code null}.
         */
        public AllTypes(int i, boolean null_val) {
            stringField = (null_val) ? null : "string_field_value" + i;
            byteArrField = (null_val) ? null : BigInteger.valueOf(i).toByteArray();
            booleanField = (i & 1) == 0;
            booleanObjField = (null_val) ? null : (i & 1) == 0;
            shortField = (short)i;
            shortObjField = (null_val) ? null : shortField;
            intField = i;
            integerField = (null_val) ? null : i;
            longField = i;
            longObjField = (null_val) ? null : longField;
            floatField = i;
            floatObjField = (null_val) ? null : floatField;
            doubleField = i;
            doubleObjField = (null_val) ? null : doubleField;
            dateField = (null_val) ? null : Date.valueOf(String.format("%04d-04-09", 1000 + i));
            timeField = (null_val) ? null : new Time(i * 1000);
            timestampField = (null_val) ? null : Timestamp.valueOf(String.format("%04d-04-09 12:00:00", 1000 + i));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "AllTypes{" +
                "stringField='" + stringField + '\'' +
                ", byteArrField=" + byteArrField +
                ", booleanField=" + booleanField +
                ", boolean_obj_field=" + booleanObjField +
                ", short_field=" + shortField +
                ", short_obj_field=" + shortObjField +
                ", int_field=" + intField +
                ", Integer_field=" + integerField +
                ", long_field=" + longField +
                ", long_obj_field=" + longObjField +
                ", float_field=" + floatField +
                ", float_obj_field=" + floatObjField +
                ", double_field=" + doubleField +
                ", double_obj_field=" + doubleObjField +
                ", date_field=" + dateField +
                ", time_field=" + timeField +
                ", timestamp_field=" + timestampField +
                '}';
        }
    }
}
