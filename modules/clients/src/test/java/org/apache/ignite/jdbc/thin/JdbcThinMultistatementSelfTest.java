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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for ddl queries that contain multiply sql statements, separated by ";".
 */
public class JdbcThinMultistatementSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override public void beforeTestsStarted() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, "false");
        
        startGrids(2);
    }

    /**
     * Setup tables.
     */
    @Before
    public void setupTables() throws Exception {
        execute("CREATE TABLE TEST_TX " +
            "(ID INT PRIMARY KEY, AGE INT, NAME VARCHAR) " +
            "WITH \"atomicity=transactional_snapshot\";");

        execute("INSERT INTO TEST_TX VALUES " +
            "(1, 17, 'James'), " +
            "(2, 43, 'Valery'), " +
            "(3, 25, 'Michel'), " +
            "(4, 19, 'Nick');");
    }

    @After
    public void dropTables () throws Exception {
        execute("DROP TABLE IF EXISTS TEST_TX; " +
            "DROP TABLE IF EXISTS public.transactions; " +
            "DROP TABLE IF EXISTS ONE;" +
            "DROP TABLE IF EXISTS TWO;");
    }


    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Execute sql script using thin driver.
     */
    private void execute(String sql) throws Exception {
        try (Connection c = GridTestUtils.connect(grid(0), null)) {
            try (Statement stmt = c.createStatement()) {
                stmt.executeUpdate(sql);
            }

        }
    }

    /**
     * Assert that script containing both h2 and non h2 (native) sql statements is handled correctly.
     */
    @Test
    public void testMixedCommands() throws Exception{
        execute("CREATE TABLE public.transactions (pk INT, id INT, k VARCHAR, v VARCHAR, PRIMARY KEY (pk, id)); " +
            "CREATE INDEX transactions_id_k_v ON public.transactions (id, k, v) INLINE_SIZE 150; " +
            "INSERT INTO public.transactions VALUES (1,2,'some', 'word') ; " +
            "CREATE INDEX transactions_k_v_id ON public.transactions (k, v, id) INLINE_SIZE 150; " +
            "CREATE INDEX transactions_pk_id ON public.transactions (pk, id) INLINE_SIZE 20;");
    }

    /**
     * Sanity test for scripts, containing empty statements are handled correctly.
     */
    @Test
    public void testEmptyStatements() throws Exception {
        execute(";; ;;;;");
        execute(" ;; ;;;; ");
        execute("CREATE TABLE ONE (id INT PRIMARY KEY, VAL VARCHAR);;" +
            "CREATE INDEX T_IDX ON ONE(val)" +
            ";;UPDATE ONE SET VAL = 'SOME';;;  ");
        execute("DROP INDEX T_IDX ;;  ;;" +
            "UPDATE ONE SET VAL = 'SOME'");
    }

    @Test
    public void testMultiStatementTx() throws Exception {
        String complexQuery =
            "INSERT INTO TEST_TX VALUES (5, 28, 'Leo'); " +
                "BEGIN; " +
                "UPDATE TEST_TX  SET name = 'Nickolas' WHERE name = 'Nick';" +
                "INSERT INTO TEST_TX VALUES (6, 84, 'Gab'); " +
                "DELETE FROM TEST_TX WHERE age < 19; " +
                "COMMIT;";

        try (Connection c = GridTestUtils.connect(grid(0), null)) {
            try (PreparedStatement p = c.prepareStatement(complexQuery)) {
                p.execute();

                assertTrue("Expected update count of the INSERT.", p.getUpdateCount() != -1);
                assertTrue("Expected update count of the BEGIN", p.getUpdateCount() != -1);
                assertTrue("Expected update count of the UPDATE", p.getUpdateCount() != -1);
                assertTrue("Expected update count of the INSERT", p.getUpdateCount() != -1);
                assertTrue("Expected update count of the DELETE", p.getUpdateCount() != -1);

                assertTrue("Expected update count of the COMMIT", p.getUpdateCount() != -1);

                assertFalse("There should have been no results.", p.getMoreResults());
                assertFalse("There should have been no update results.", p.getUpdateCount() != -1);
            }
        }
    }
}
