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
 */

package org.apache.ignite.internal.processors.cache.index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test of creating and using PK indexes for tables created through SQL.
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
public class ComplexPrimaryKeyUnwrapSelfTest extends GridCommonAbstractTest {

    /** Counter to generate unique table names. */
    private static int tblCnt = 0;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Test using PK indexes for complex primary key.
     */
    public void testComplexPk() {
        String tblName = createTableName();

        executeSql("CREATE TABLE " + tblName + " (id int, name varchar, age int, company varchar, city varchar, " +
            "primary key (id, name, city))");

        checkUsingIndexes(tblName, "1");
    }

    /**
     * Test using PK indexes for simple primary key.
     */
    public void testSimplePk() {
        //ToDo: IGNITE-8386: need to add DATE type into the test.
        HashMap<String, String> types = new HashMap() {
            {
                put("boolean", "1");
                put("char", "'1'");
                put("varchar", "'1'");
                put("real", "1");
                put("number", "1");
                put("int", "1");
                put("long", "1");
                put("float", "1");
                put("double", "1");
                put("tinyint", "1");
                put("smallint", "1");
                put("bigint", "1");
                put("varchar_ignorecase", "'1'");
                put("time", "'11:11:11'");
                put("timestamp", "'20018-11-02 11:11:11'");
                put("uuid", "'1'");
            }
        };
        for (Map.Entry<String, String> entry : types.entrySet()) {

            String tblName = createTableName();

            String type = entry.getKey();
            String value = entry.getValue();

            executeSql("CREATE TABLE " + tblName +
                " (id " + type + " , name varchar, age int, company varchar, city varchar," +
                " primary key (id))");

            checkUsingIndexes(tblName, value);
        }
    }

    /**
     * Test using PK indexes for simple primary key and affinity key.
     */
    public void testSimplePkWithAffinityKey() {
        //ToDo: IGNITE-8386: need to add DATE type into the test.
        HashMap<String, String> types = new HashMap() {
            {
                put("boolean", "1");
                put("char", "'1'");
                put("varchar", "'1'");
                put("real", "1");
                put("number", "1");
                put("int", "1");
                put("long", "1");
                put("float", "1");
                put("double", "1");
                put("tinyint", "1");
                put("smallint", "1");
                put("bigint", "1");
                put("varchar_ignorecase", "'1'");
                put("time", "'11:11:11'");
                put("timestamp", "'20018-11-02 11:11:11'");
                put("uuid", "'1'");
            }
        };
        for (Map.Entry<String, String> entry : types.entrySet()) {

            String tblName = createTableName();

            String type = entry.getKey();
            String value = entry.getValue();

            executeSql("CREATE TABLE " + tblName +
                " (id " + type + " , name varchar, age int, company varchar, city varchar," +
                " primary key (id)) WITH \"affinity_key=id\"");

            checkUsingIndexes(tblName, value);
        }
    }

    /**
     * Test using PK indexes for wrapped primary key.
     */
    public void testWrappedPk() {
        String tblName = createTableName();

        executeSql("CREATE TABLE " + tblName + " (id int, name varchar, age int, company varchar, city varchar, " +
            "primary key (id)) WITH \"wrap_key=true\"");

        checkUsingIndexes(tblName, "1");
    }

    /**
     * Check using PK indexes for few cases.
     *
     * @param tblName name of table which should be checked to using PK indexes.
     */
    private void checkUsingIndexes(String tblName, String idValue) {
        String explainSQL = "explain SELECT * FROM " + tblName + " WHERE ";

        List<List<?>> results = executeSql(explainSQL + "id=" + idValue);

        assertUsingPkIndex(results);

        results = executeSql(explainSQL + "id=" + idValue + " and name=''");

        assertUsingPkIndex(results);

        results = executeSql(explainSQL + "id=" + idValue + " and name='' and city='' and age=0");

        assertUsingPkIndex(results);
    }

    /**
     * Test don't using PK indexes for table created through cache API.
     */
    public void testIndexesForCachesCreatedThroughCashApi() {
        String tblName = TestValue.class.getSimpleName();

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setSqlSchema("PUBLIC");
        ccfg.setName(tblName);

        QueryEntity qryEntity = new QueryEntity(TestKey.class, TestValue.class);

        ccfg.setQueryEntities(F.asList(qryEntity));

        node().createCache(ccfg);

        List<List<?>> results = executeSql("explain SELECT * FROM " + tblName + " WHERE id=1");

        assertDontUsingPkIndex(results);
    }

    /**
     * Check that explain plan result shown using PK index and don't use scan.
     *
     * @param results result of execut explain plan query.
     */
    private void assertUsingPkIndex(List<List<?>> results) {
        assertEquals(2, results.size());

        String explainPlan = (String)results.get(0).get(0);

        assertTrue(explainPlan.contains("\"_key_PK"));

        assertFalse(explainPlan.contains("_SCAN_"));
    }

    /**
     * Check that explain plan result shown don't use PK index and use scan.
     *
     * @param results result of execut explain plan query.
     */
    private void assertDontUsingPkIndex(List<List<?>> results) {
        assertEquals(2, results.size());

        String explainPlan = (String)results.get(0).get(0);

        System.out.println(explainPlan);

        assertFalse(explainPlan.contains("\"_key_PK\""));

        assertTrue(explainPlan.contains("_SCAN_"));
    }

    /**
     * Create unique table name.
     *
     * @return unique name of table.
     */
    private String createTableName() {
        return "TST_TABLE_" + tblCnt++;
    }

    /**
     * Run SQL statement on default node.
     *
     * @param stmt Statement to run.
     * @param args arguments of statements
     * @return Run result.
     */
    private List<List<?>> executeSql(String stmt, Object... args) {
        return executeSql(node(), stmt, args);
    }

    /**
     * Run SQL statement on specified node.
     *
     * @param node node to execute query.
     * @param stmt Statement to run.
     * @param args arguments of statements
     * @return Run result.
     */
    private List<List<?>> executeSql(IgniteEx node, String stmt, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
    }

    /**
     * @return Node to initiate operations from.
     */
    private IgniteEx node() {
        return grid(0);
    }

    /**
     *
     */
    static class TestKey {
        /** */
        @QuerySqlField
        private int id;

        /**
         * @param id ID.
         */
        public TestKey(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey testKey = (TestKey)o;

            return id == testKey.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    static class TestValue {
        /** */
        @QuerySqlField()
        private String name;
        /** */
        @QuerySqlField()
        private String company;
        /** */
        @QuerySqlField()
        private String city;
        /** */
        @QuerySqlField()
        private int age;

        /**
         * @param age Age.
         * @param name Name.
         * @param company Company.
         * @param city City.
         */
        public TestValue(int age, String name, String company, String city) {
            this.age = age;
            this.name = name;
            this.company = company;
            this.city = city;
        }
    }

}
