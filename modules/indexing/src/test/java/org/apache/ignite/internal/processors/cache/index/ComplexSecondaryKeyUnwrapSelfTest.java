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

package org.apache.ignite.internal.processors.cache.index;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/**
 * Test of creating and using secondary indexes for tables created through SQL.
 */
@SuppressWarnings({"ThrowableResultOfMethodCallIgnored"})
public class ComplexSecondaryKeyUnwrapSelfTest extends AbstractIndexingCommonTest {
    /** Counter to generate unique table names. */
    private static int tblCnt;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /**
     * Test secondary index with complex PK. Columns for secondary and PK indexes are intersect.
     */
    @Test
    public void testSecondaryIndexWithIntersectColumnsComplexPk() {
        String tblName = createTableName();

        executeSql("CREATE TABLE " + tblName + " (id int, name varchar, age int, company varchar, city varchar, " +
            "primary key (name, city))");

        executeSql("CREATE INDEX ON " + tblName + "(id, name, city)");

        checkUsingIndexes(tblName, "'1'", 2, "Query should be splitted");
        checkUsingIndexesWithTwoStepPlan(tblName, "Query should be splitted");
    }

    /**
     * Test using secondary index with simple PK.
     */
    @Test
    public void testSecondaryIndexSimplePk() {
        Map<String, String> types = new LinkedHashMap<String, String>() {
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
            String val = entry.getValue();

            executeSql("CREATE TABLE " + tblName +
                " (id int, name " + type + ", age int, company varchar, city varchar," +
                " primary key (name))");

            executeSql("CREATE INDEX ON " + tblName + "(id, name, city)");

            checkUsingIndexes(tblName, val, 1, "Query with type column: " + type);
            checkUsingIndexesWithTwoStepPlan(tblName, "Query with type column: " + type + " should be splitted");
        }
    }

    /**
     * Check using secondary indexes for few cases.
     *
     * @param tblName name of table which should be checked to using secondary indexes.
     * @param nameVal Value for name param.
     * @param expResCnt Expected number of elements in result plan.
     * @param assertLbl Assert label.
     */
    private void checkUsingIndexes(String tblName, String nameVal, int expResCnt, String assertLbl) {
        String explainSQL = "explain SELECT * FROM " + tblName + " WHERE ";

        List<List<?>> results = executeSql(explainSQL + "id=1 and name=" + nameVal);

        assertUsingSecondaryIndex(results, expResCnt, assertLbl);

        results = executeSql(explainSQL + "id=1 and name=" + nameVal + " and age=0");

        assertUsingSecondaryIndex(results, expResCnt, assertLbl);
    }

    /**
     * Check using secondary indexes with two-step plan.
     *
     * @param tblName name of table which should be checked to using secondary indexes.
     * @param assertLbl Assert label.
     */
    private void checkUsingIndexesWithTwoStepPlan(String tblName, String assertLbl) {
        String explainSQL = "explain SELECT * FROM " + tblName + " WHERE ";

        List<List<?>> results = executeSql(explainSQL + "id=1");

        // Always used merge_scan for non key (or affinity) fields condition.
        assertUsingSecondaryIndex(results, 2, assertLbl);
    }

    /**
     * Check that explain plan result shown using Secondary index and don't use scan.
     *
     * @param results result of execut explain plan query.
     * @param expResCnt Expceted result count.
     * @param assertLbl Assert label.
     */
    private void assertUsingSecondaryIndex(List<List<?>> results, int expResCnt, String assertLbl) {
        assertEquals(assertLbl, expResCnt, results.size());

        String explainPlan = (String)results.get(0).get(0);

        assertTrue(explainPlan, explainPlan.contains("_idx\": "));

        assertFalse(explainPlan, explainPlan.contains("_SCAN_"));
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
}
