
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

package org.apache.ignite.compatibility.persistence;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Test;

/**
 * Test to check that starting node with PK index of the old format present doesn't break anything.
 */
public class IgnitePKIndexesMigrationToAlternativeKeyTest extends IndexingMigrationAbstractionTest {
    /** */
    private static String TABLE_NAME = "TEST_IDX_TABLE";

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSecondaryIndexesMigration_2_7() throws Exception {
        doTestStartupWithOldVersion("2.7.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSecondaryIndexesMigration_2_5() throws Exception {
        doTestStartupWithOldVersion("2.5.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSecondaryIndexesMigration_2_4() throws Exception {
        doTestStartupWithOldVersion("2.4.0");
    }

    /**
     * Tests opportunity to read data from previous Ignite DB version.
     *
     * @param ver 3-digits version of ignite
     * @throws Exception If failed.
     */
    private void doTestStartupWithOldVersion(String ver) throws Exception {
        try {
            startGrid(1, ver, new ConfigurationClosure(), new PostStartupClosure(true));

            stopAllGrids();

            IgniteEx igniteEx = startGrid(0);

            new PostStartupClosure(false).apply(igniteEx);

            igniteEx.cluster().active(true);

            assertDontUsingPkIndex(igniteEx, TABLE_NAME);

            String newTblName = TABLE_NAME + "_NEW";

            initializeTable(igniteEx, newTblName);

            checkUsingIndexes(igniteEx, newTblName);

            igniteEx.cluster().active(false);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {

        /**
         *
         */
        boolean createTable;

        /**
         * @param createTable {@code true} In case table should be created
         */
        public PostStartupClosure(boolean createTable) {
            this.createTable = createTable;
        }

        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.cluster().active(true);

            IgniteEx igniteEx = (IgniteEx)ignite;

            if (createTable)
                initializeTable(igniteEx, TABLE_NAME);

            assertDontUsingPkIndex(igniteEx, TABLE_NAME);

            ignite.cluster().active(false);
        }
    }

    /**
     * @param igniteEx Ignite instance.
     * @param tblName Table name.
     */
    private static void initializeTable(IgniteEx igniteEx, String tblName) {
        executeSql(igniteEx, "CREATE TABLE " + tblName + " (id int, name varchar, age int, company varchar, city varchar, " +
            "primary key (id, name, city)) WITH \"affinity_key=name\"");

        executeSql(igniteEx, "CREATE INDEX ON " + tblName + "(city, id)");

        for (int i = 0; i < 1000; i++)
            executeSql(igniteEx, "INSERT INTO " + tblName + " (id, name, age, company, city) VALUES(0,'name"+ i + "',2,'company', 'city"+ i + "')", i);
    }

    /**
     * Run SQL statement on specified node.
     *
     * @param node node to execute query.
     * @param stmt Statement to run.
     * @param args arguments of statements
     * @return Run result.
     */
    private static List<List<?>> executeSql(IgniteEx node, String stmt, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
    }

    /**
     * Check using PK indexes for few cases.
     *
     * @param ignite Ignite instance.
     * @param tblName name of table which should be checked to using PK indexes.
     */
    private static void checkUsingIndexes(IgniteEx ignite, String tblName) {

        for (int i = 0; i < 1000; i++) {
            String s = "SELECT name FROM " + tblName + " WHERE city="+ i + " AND id=0 AND name='name"+ i + "'";
            System.out.println(executeSql(ignite, s));
        }

    }

    /**
     * Check that explain plan result shown using PK index and don't use scan.
     *
     * @param results Result list of explain of query.
     */
    private static void assertUsingPkIndex(List<List<?>> results) {
        String explainPlan = (String)results.get(0).get(0);

        assertTrue(explainPlan.contains("\"_key_PK"));

        assertFalse(explainPlan.contains("_SCAN_"));
    }

    /**
     * Check that explain plan result shown don't use PK index and use scan.
     *
     * @param igniteEx Ignite instance.
     * @param tblName Name of table.
     */
    private static void assertDontUsingPkIndex(IgniteEx igniteEx, String tblName) {
        List<List<?>> results = executeSql(igniteEx, "explain SELECT * FROM " + tblName + " WHERE id=1");

        assertEquals(2, results.size());

        String explainPlan = (String)results.get(0).get(0);

        System.out.println(explainPlan);

        assertFalse(explainPlan, explainPlan.contains("\"_key_PK\""));

        assertTrue(explainPlan, explainPlan.contains("_SCAN_"));
    }

}
