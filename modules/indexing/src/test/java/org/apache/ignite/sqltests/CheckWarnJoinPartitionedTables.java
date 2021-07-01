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

package org.apache.ignite.sqltests;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Check that illegal joins of partitioned tables are warned. */
public class CheckWarnJoinPartitionedTables extends GridCommonAbstractTest {
    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** */
    private Ignite crd;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        crd = startGrid();
    }

    /** */
    @Test
    public void joinSameTableWithPrimaryKey() {
        execute(crd, new SqlFieldsQuery(
            "CREATE TABLE A (ID INT PRIMARY KEY, TITLE VARCHAR);"));

        checkSameTableWithJoinType("LEFT JOIN");
        checkSameTableWithJoinType("RIGHT JOIN");
        checkSameTableWithJoinType("INNER JOIN");
        checkSameTableWithJoinType("JOIN");
    }

    /** */
    @Test
    public void joinSameTableWithPrimaryAffinityKey() {
        execute(crd, new SqlFieldsQuery(
            "CREATE TABLE A (ID INT PRIMARY KEY, TITLE VARCHAR) with \"AFFINITY_KEY=ID\";"));

        checkSameTableWithJoinType("LEFT JOIN");
        checkSameTableWithJoinType("RIGHT JOIN");
        checkSameTableWithJoinType("INNER JOIN");
        checkSameTableWithJoinType("JOIN");
    }

    /** */
    private void checkSameTableWithJoinType(String joinType) {
        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a2.ID = a1.ID;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2._KEY;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2.ID;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2._KEY;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID = a2.ID;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 where a1.ID = a2._KEY;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID AND a1.TITLE = a2.TITLE;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID AND a1.TITLE != a2.TITLE;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID AND (a1.TITLE = a2.TITLE OR a1.ID = a2.ID);");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on (a1.TITLE = a2.TITLE OR a1.ID = a2.ID) AND a1.ID = a2.ID;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " (select a2.ID from A a2) a3 on a1.ID = a3.ID;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 where a1.ID in (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.ID);");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.ID) t" +
                " on a1.ID = t.ID;");

        // Ignite doesn't support this.
        if (!joinType.contains("RIGHT"))
            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID " + joinType + " A a3 on a1.ID = a3.ID;");

        // Some simple illegal joins.

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on 1 = 1;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID > a2.ID;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.TITLE;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2.TITLE;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.TITLE = a2.TITLE;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID OR a1.TITLE = a2.TITLE;");

        // Actually this is correct query. But this AST is too complex to analyze, and also it could be simplified.
        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID OR (a1.ID = a2.ID AND a1.TITLE = a2.TITLE);");

        if (!joinType.contains("RIGHT"))
            checkLogListener(true,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.TITLE " + joinType + " A a3 on a1.ID = a3.ID;");

        // This query is invalid. But we don't have info about partitioning in nested joins.
        if (!joinType.contains("RIGHT")) {
            // For inner join we can check it because join conditions are parsed to WHERE clause.
            boolean shouldWarn = !joinType.contains("LEFT");

            checkLogListener(shouldWarn,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID " + joinType + " A a3 on a1.ID = a3.TITLE;");
        }

        checkLogListener(true,
            "SELECT a1.* FROM A a1 where a1.ID in (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.TITLE);");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 where a1.ID in (select a2.ID from A a2 " + joinType + " A a3 on a2.ID != a3.ID);");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.TITLE) t" +
                " on a1.ID = t.ID;");
    }

    /** */
    @Test
    public void joinSameTableWithComplexPrimaryKey() {
        execute(crd, new SqlFieldsQuery(
            "CREATE TABLE A (ID INT, TITLE VARCHAR, PRICE INT, COMMENT VARCHAR, PRIMARY KEY (ID, TITLE, PRICE));"));

        checkSameTableWithComplexPrimaryKeyWithJoinType("LEFT JOIN");
        checkSameTableWithComplexPrimaryKeyWithJoinType("RIGHT JOIN");
        checkSameTableWithComplexPrimaryKeyWithJoinType("INNER JOIN");
        checkSameTableWithComplexPrimaryKeyWithJoinType("JOIN");
    }

    /** */
    private void checkSameTableWithComplexPrimaryKeyWithJoinType(String joinType) {
        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2._KEY;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a2._KEY = a1._KEY;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on " +
                "a1.ID = a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE and a1.COMMENT = a2.COMMENT;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on " +
                "a1.ID = a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE and a1.COMMENT != a2.COMMENT;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.PRICE = a2.PRICE and a1.TITLE = a2.TITLE and a1.ID = a2.ID;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID where a1.PRICE = a2.PRICE and a1.TITLE = a2.TITLE;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID where a1.PRICE = a2.PRICE and a1.TITLE = a2.TITLE;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 where a1.ID in (" +
                "   select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.ID and a2.TITLE = a3.TITLE and a2.PRICE = a3.PRICE);");

        if (!joinType.contains("RIGHT"))
            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2._KEY " + joinType + " A a3 on a1._KEY = a3._KEY;");

        // Some simple illegal joins.

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a2._KEY != a1._KEY;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID and a1.TITLE != a2.TITLE and a1.PRICE != a2.PRICE;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID and a1.PRICE = a2.PRICE;");

        if (!joinType.contains("RIGHT")) {
            // For inner join we can check it because join conditions are parsed to WHERE clause.
            boolean shouldWarn = !joinType.contains("LEFT");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2._KEY " + joinType + " A a3 on a1._KEY != a3._KEY;");

            checkLogListener(!shouldWarn,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY != a2._KEY " + joinType + " A a3 on a1._KEY = a3._KEY;");
        }
    }

    /** */
    @Test
    public void joinSameTableWithComplexPrimaryKeySingleAffKey() {
        execute(crd, new SqlFieldsQuery(
            "CREATE TABLE A (ID INT, TITLE VARCHAR, PRICE INT, COMMENT VARCHAR, PRIMARY KEY (ID, TITLE, PRICE))" +
                " with \"AFFINITY_KEY=ID\";"));

        checkSameTableWithComplexPrimaryKeySingleAffKeyWithJoinType("LEFT JOIN");
        checkSameTableWithComplexPrimaryKeySingleAffKeyWithJoinType("RIGHT JOIN");
        checkSameTableWithComplexPrimaryKeySingleAffKeyWithJoinType("INNER JOIN");
        checkSameTableWithComplexPrimaryKeySingleAffKeyWithJoinType("JOIN");
    }

    /** */
    private void checkSameTableWithComplexPrimaryKeySingleAffKeyWithJoinType(String joinType) {
        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID and a1.TITLE != a2.TITLE;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.TITLE != a2.TITLE and a1.ID = a2.ID;");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 where a1.ID in (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.ID);");

        checkLogListener(false,
            "SELECT a1.* FROM A a1 " + joinType + " (select a2.ID from A a2 " + joinType + " A a3 on a2.ID = a3.ID) t" +
                " on a1.ID = t.ID;");

        if (!joinType.contains("RIGHT"))
            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID " + joinType + " A a3 on a1.ID = a3.ID;");

        // Some simple illegal joins.

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1._KEY = a2._KEY;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID and a1.TITLE = a2.TITLE and a1.PRICE = a2.PRICE;");

        checkLogListener(true,
            "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.TITLE = a2.TITLE;");

        if (!joinType.contains("RIGHT")) {
            // For inner join we can check it because join conditions are parsed to WHERE clause.
            boolean shouldWarn = !joinType.contains("LEFT");

            checkLogListener(false,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID = a2.ID " + joinType + " A a3 on a1.ID != a3.ID;");

            checkLogListener(!shouldWarn,
                "SELECT a1.* FROM A a1 " + joinType + " A a2 on a1.ID != a2.ID " + joinType + " A a3 on a1.ID = a3.ID;");
        }
    }

    /** */
    @Test
    public void joinWithPrimaryKey() {
        execute(crd, new SqlFieldsQuery(
            "CREATE TABLE A (ID INT PRIMARY KEY, TITLE VARCHAR);"));

        execute(crd, new SqlFieldsQuery(
            "CREATE TABLE B (ID INT PRIMARY KEY, PRICE INT);"));

        checkJoinPrimaryKeyWithJoinType("LEFT JOIN");
        checkJoinPrimaryKeyWithJoinType("RIGHT JOIN");
        checkJoinPrimaryKeyWithJoinType("INNER JOIN");
        checkJoinPrimaryKeyWithJoinType("JOIN");
    }

    /** */
    private void checkJoinPrimaryKeyWithJoinType(String joinType) {
        checkLogListener(false,
            "SELECT a.* FROM A a " + joinType + " B b on a.ID = b.ID;");

        checkLogListener(false,
            "SELECT a.* FROM A a " + joinType + " B b on b.ID = a.ID;");

        checkLogListener(false,
            "SELECT a.* FROM A a " + joinType + " B b on b._KEY = a._KEY;");

        checkLogListener(false,
            "SELECT a.* FROM A a " + joinType + " B b on b.ID = a.ID and a.TITLE != 'Title';");

        checkLogListener(false,
            "SELECT a.* FROM A a " + joinType + " B b on b.ID = a.ID and b.PRICE > 100;");

        checkLogListener(false,
            "SELECT a.* FROM A a " + joinType + " B b on b.ID = a.ID and b.PRICE != a.ID;");

        // Some wrong queries.

        checkLogListener(true,
            "SELECT a.* FROM A a " + joinType + " B b on 1 = 1;");

        checkLogListener(true,
            "SELECT a.* FROM A a " + joinType + " B b on a.ID != b.ID;");

        checkLogListener(true,
            "SELECT a.* FROM A a " + joinType + " B b on b.ID = 1;");

        checkLogListener(true,
            "SELECT a.* FROM A a " + joinType + " B b on a.ID = b.PRICE;");
    }


    /** */
    @Test
    // This test should not pass as it contains wrong query. But current check pass it.
    public void testWrongQueryButAllAffinityKeysAreUsed() {
        execute(crd, new SqlFieldsQuery(
            "CREATE TABLE A (ID INT, TITLE VARCHAR, PRICE INT, COMMENT VARCHAR, PRIMARY KEY (ID, TITLE, PRICE));"));

        // PRICE = ID
        checkLogListener(false,
                "SELECT a1.* FROM A a1 LEFT JOIN A a2 on a1.PRICE = a2.ID and a1.TITLE = a2.TITLE and a1.ID = a2.PRICE;");
    }

    /** */
    private void checkLogListener(boolean shouldFindMsg, String sql) {
        LogListener lsnr = LogListener.matches(
            "For join two partitioned tables join condition should be the equality operation of affinity keys"
        ).build();

        testLog.registerListener(lsnr);

        execute(crd, new SqlFieldsQuery(sql));

        if (shouldFindMsg)
            assertTrue(sql, lsnr.check());
        else
            assertFalse(sql, lsnr.check());

        testLog.clearListeners();
    }

    /**
     * Execute query from node.
     *
     * @param node node to use to perform query.
     * @param qry query.
     */
    protected final void execute(Ignite node, SqlFieldsQuery qry) {
        FieldsQueryCursor<List<?>> cursor = ((IgniteEx)node).context().query().querySqlFields(qry, false);

        cursor.getAll();
    }
}
