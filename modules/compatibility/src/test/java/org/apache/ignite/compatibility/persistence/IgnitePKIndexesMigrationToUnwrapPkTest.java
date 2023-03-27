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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Test;

/**
 * Test to check that starting node with PK index of the old format present doesn't break anything.
 */
public class IgnitePKIndexesMigrationToUnwrapPkTest extends IndexAbstractCompatibilityTest {
    /** */
    private static final String TABLE_NAME = "TEST_IDX_TABLE";

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
            startGrid(1, ver, new PersistenceBasicCompatibilityTest.ConfigurationClosure(true),
                new PostStartupClosure());

            stopAllGrids();

            IgniteEx igniteEx = startGrid(0);

            igniteEx.cluster().state(ClusterState.ACTIVE);

            assertDontUsingPkIndex(igniteEx, TABLE_NAME);

            assertQueryWorks(igniteEx, TABLE_NAME);

            String newTblName = TABLE_NAME + "_NEW";

            initializeTable(igniteEx, newTblName);

            checkUsingIndexes(igniteEx, newTblName);

            assertQueryWorks(igniteEx, newTblName);

            igniteEx.cluster().state(ClusterState.INACTIVE);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            IgniteEx igniteEx = (IgniteEx)ignite;

            initializeTable(igniteEx, TABLE_NAME);

            assertDontUsingPkIndex(igniteEx, TABLE_NAME);

            ignite.active(false);
        }
    }

    /**
     * @param igniteEx Ignite instance.
     * @param tblName Table name.
     */
    private static void initializeTable(IgniteEx igniteEx, String tblName) {
        executeSql(igniteEx, "CREATE TABLE " + tblName + " (id int, name varchar, age int, company varchar, city varchar, " +
            "primary key (id, name, city)) WITH \"affinity_key=name\"");

        executeSql(igniteEx, "CREATE INDEX ON " + tblName + "(city, age)");

        for (int i = 0; i < 1000; i++)
            executeSql(igniteEx, "INSERT INTO " + tblName + " (id, name, age, company, city) VALUES(?,'name',2,'company', 'city')", i);
    }

    /**
     * Check using PK indexes for few cases.
     *
     * @param ignite Ignite instance.
     * @param tblName name of table which should be checked to using PK indexes.
     */
    private static void checkUsingIndexes(IgniteEx ignite, String tblName) {
        String explainSQL = "explain SELECT * FROM " + tblName + " WHERE ";

        List<List<?>> results = executeSql(ignite, explainSQL + "id=1");

        assertUsingPkIndex(results);

        results = executeSql(ignite, explainSQL + "id=1 and name='name'");

        assertUsingPkIndex(results);

        results = executeSql(ignite, explainSQL + "id=1 and name='name' and city='city' and age=2");

        assertUsingPkIndex(results);
    }

    /**
     * Check that explain plan result shown using PK index and don't use scan.
     *
     * @param results Result list of explain of query.
     */
    private static void assertUsingPkIndex(List<List<?>> results) {
        assertFalse(results.isEmpty());

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

        assertFalse(results.isEmpty());

        String explainPlan = (String)results.get(0).get(0);

        System.out.println(explainPlan);

        assertFalse(explainPlan, explainPlan.contains("\"_key_PK\""));

        assertTrue(explainPlan, explainPlan.contains("_SCAN_"));
    }

    /**
     * Check that query on table works correctly.
     *
     * @param igniteEx Ignite instance.
     * @param tblName Name of table.
     */
    private static void assertQueryWorks(IgniteEx igniteEx, String tblName) {
        List<List<?>> results = executeSql(igniteEx, "SELECT * FROM " + tblName + " WHERE id=1");

        assertEquals(1, results.size());
        assertEquals(1, results.get(0).get(0));
    }
}
