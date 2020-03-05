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

package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class StatementCacheTest extends AbstractIndexingCommonTest {
    /** */
    private static final int SIZE_BIG = 1000;

    /** */
    private static final int SIZE_SMALL = 10;

    /** Enforce join order. */
    private boolean enforceJoinOrder;

    /** Local flag . */
    private boolean local;

    /** Distributed join flag . */
    private boolean distributedJoin;

    /** Collocated GROUP BY flag . */
    private boolean collocatedGroupBy;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        local = false;
        enforceJoinOrder = false;
        distributedJoin = false;
        collocatedGroupBy = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     *
     */
    public void testEnforceJoinOrderFlag() throws Exception {
        startGrids(1);

        sql("CREATE TABLE TBL0 (ID INT PRIMARY KEY, JID INT, VAL INT)");
        sql("CREATE TABLE TBL1 (ID INT PRIMARY KEY, JID INT, VAL INT)");

        sql("CREATE INDEX IDX_TBL1_JID ON TBL1(JID)");

        sql("INSERT INTO TBL0 VALUES (0, 0, 0)");

        for (int i = 0; i < SIZE_BIG; ++i)
            sql("INSERT INTO TBL1 VALUES (?, ?, ?)", i, i + 1, i);

        local = true;

        sql("EXPLAIN SELECT TBL1.ID FROM TBL1, TBL0 WHERE TBL0.JID=TBL1.JID").getAll();

        enforceJoinOrder = true;

        String plan = (String)sql("EXPLAIN SELECT TBL1.ID FROM TBL1, TBL0 WHERE TBL0.JID=TBL1.JID").getAll().get(0).get(0);

        assertTrue("Invalid join order: " + plan ,
            Pattern.compile("PUBLIC.TBL1[\\n\\w\\W]+PUBLIC.TBL0", Pattern.MULTILINE)
                .matcher(plan).find());
    }

    /**
     *
     */
    public void testDistributedJoinFlag() throws Exception {
        startGrids(3);

        sql("CREATE TABLE TBL0 (ID INT PRIMARY KEY, JID INT, VAL INT)");
        sql("CREATE TABLE TBL1 (ID INT PRIMARY KEY, JID INT, VAL INT)");

        sql("CREATE INDEX IDX_TBL0_JID ON TBL0(JID)");
        sql("CREATE INDEX IDX_TBL1_JID ON TBL1(JID)");

        for (int i = 0; i < SIZE_SMALL; ++i)
            sql("INSERT INTO TBL0 VALUES (?, ?, ?)", i + 1, i + 1, i + 1);

        for (int i = 0; i < SIZE_BIG; ++i)
            sql("INSERT INTO TBL1 VALUES (?, ?, ?)", i, i + 1, i);

        distributedJoin = false;
        // Warm-up statements cache
        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 100; ++i)
                sql("SELECT TBL1.ID FROM TBL1, TBL0 WHERE TBL0.JID=TBL1.JID").getAll();
            }, 20, "statements-cache-warmup");

        int size = sql("SELECT TBL1.ID FROM TBL1, TBL0 WHERE TBL0.JID=TBL1.JID").getAll().size();

        distributedJoin = true;
        int distSize = sql("SELECT TBL1.ID FROM TBL1, TBL0 WHERE TBL0.JID=TBL1.JID").getAll().size();

        assertEquals("Invalid result set size of distributed join", SIZE_SMALL, distSize);
        assertTrue("Invalid result set size of collocated join: " + size, size > 0 && size < distSize);
    }

    /**
     *
     */
    public void testCollocatedFlag() throws Exception {
        startGrids(3);

        Object [][] rows = new Object[][] {
            new Object[] {1, "Bob", 10, "Shank"},
            new Object[] {2, "Bob", 10, "Shank"},
            new Object[] {3, "Bob", 10, "Shank"},
            new Object[] {4, "Bill", 10, "Shank"},
            new Object[] {5, "Bill", 10, "Shank"},
            new Object[] {6, "Bill", 10, "Shank"},
        };

        sql("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME0 VARCHAR, AGE INT, NAME1 VARCHAR)");

        for (Object[] row : rows)
            sql("INSERT INTO TEST VALUES (?, ?, ?, ?)", row);

        collocatedGroupBy = false;
        // Warm-up statements cache
        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < 100; ++i) {
                sql("SELECT SUM(AGE), NAME0 FROM TEST GROUP BY name0").getAll();
                sql("SELECT SUM(AGE), NAME1 FROM TEST GROUP BY name1").getAll();
            }
        }, 20, "statements-cache-warmup");

        int size = sql("SELECT SUM(AGE), NAME0 FROM TEST GROUP BY name0").getAll().size();

        collocatedGroupBy = true;

        int collSize = sql("SELECT SUM(AGE), NAME0 FROM TEST GROUP BY name0").getAll().size();

        assertEquals("Invalid group by results", 2, size);
        assertTrue("Result set of collocated query must be invalid on not collocated data", collSize > size);

        collocatedGroupBy = false;

        size = sql("SELECT SUM(AGE), NAME1 FROM TEST GROUP BY name1").getAll().size();

        collocatedGroupBy = true;

        collSize = sql("SELECT SUM(AGE), NAME1 FROM TEST GROUP BY name1").getAll().size();

        assertEquals("Invalid group by results", 1, size);
        assertTrue("Result set of collocated query must be invalid on not collocated data", collSize > size);
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return sql(grid(0), sql, args);
    }

    /**
     * @param ign Node.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLocal(local)
            .setEnforceJoinOrder(enforceJoinOrder)
            .setDistributedJoins(distributedJoin)
            .setCollocated(collocatedGroupBy)
            .setArgs(args), false);
    }
}
