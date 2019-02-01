/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.persistence.scenarios.util;

import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.compatibility.Since;
import org.junit.Assert;

/**
 * Primary index migration utils.
 */
@Since("2.4.0")
public class PdsCompatibilityWithPKIndexMigrationUtils {
    /**
     * @param igniteEx Ignite instance.
     * @param tblName Table name.
     */
    public static void initializeTable(IgniteEx igniteEx, String tblName) {
        executeSql(igniteEx, "CREATE TABLE " + tblName + " (id int, name varchar, age int, company varchar, city varchar, " +
            "primary key (id, name, city))");

        executeSql(igniteEx, "INSERT INTO " + tblName + " (id, name, age, company, city) VALUES(1,'name',2,'company', 'city')");
    }

    /**
     * Run SQL statement on specified node.
     *
     * @param node node to execute query.
     * @param stmt Statement to run.
     * @param args arguments of statements
     * @return Run result.
     */
    public static List<List<?>> executeSql(IgniteEx node, String stmt, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
    }

    /**
     * Check using PK indexes for few cases.
     *
     * @param ignite Ignite instance.
     * @param tblName name of table which should be checked to using PK indexes.
     */
    public static void checkUsingIndexes(IgniteEx ignite, String tblName) {
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
        Assert.assertEquals(2, results.size());

        String explainPlan = (String)results.get(0).get(0);

        Assert.assertTrue(explainPlan.contains("\"_key_PK"));

        Assert.assertFalse(explainPlan.contains("_SCAN_"));
    }

    /**
     * Check that explain plan result shown don't use PK index and use scan.
     *
     * @param igniteEx Ignite instance.
     * @param tblName Name of table.
     */
    public static void assertDontUsingPkIndex(IgniteEx igniteEx, String tblName) {
        List<List<?>> results = executeSql(igniteEx, "explain SELECT * FROM " + tblName + " WHERE id=1");

        Assert.assertEquals(2, results.size());

        String explainPlan = (String)results.get(0).get(0);

        System.out.println(explainPlan);

        Assert.assertFalse(explainPlan, explainPlan.contains("\"_key_PK\""));

        Assert.assertTrue(explainPlan, explainPlan.contains("_SCAN_"));
    }
}
