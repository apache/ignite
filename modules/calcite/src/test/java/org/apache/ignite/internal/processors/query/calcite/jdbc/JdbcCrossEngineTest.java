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

package org.apache.ignite.internal.processors.query.calcite.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Cross check queries on experimental and non-experimental SQL engines.
 */
public class JdbcCrossEngineTest extends GridCommonAbstractTest {
    /** URL. */
    private static final String url = "jdbc:ignite:thin://127.0.0.1?useExperimentalQueryEngine=";

    /** Nodes count. */
    private static final int nodesCnt = 3;

    /** Connections. */
    private final Connection[] conns = new Connection[2];

    /** Statements. */
    private final Statement[] stmts = new Statement[2];

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(nodesCnt);

        for (int i = 0; i < conns.length; i++) {
            conns[i] = DriverManager.getConnection(url + (i == 0 ? "false" : "true"));
            conns[i].setSchema("PUBLIC");
            stmts[i] = conns[i].createStatement();

            assert stmts[i] != null;
            assert !stmts[i].isClosed();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < conns.length; i++) {
            if (stmts[i] != null && !stmts[i].isClosed()) {
                stmts[i].close();

                assert stmts[i].isClosed();
            }

            conns[i].close();

            assert stmts[i].isClosed();
            assert conns[i].isClosed();
        }

        stopAllGrids();
    }

    /** */
    @Test
    public void testInsertDefaultValue() {
        // Test only types supported by both SQL engines.
        checkInsertDefaultValue("BOOLEAN", "TRUE", Boolean.TRUE);
        checkInsertDefaultValue("BOOLEAN NOT NULL", "TRUE", Boolean.TRUE);
        checkInsertDefaultValue("BIGINT", "10", 10L);
        checkInsertDefaultValue("INTEGER", "10", 10);
        checkInsertDefaultValue("SMALLINT", "10", (short)10);
        checkInsertDefaultValue("TINYINT", "10", (byte)10);
        checkInsertDefaultValue("DOUBLE", "10.01", 10.01d);
        checkInsertDefaultValue("REAL", "10.01", 10.01f);
        checkInsertDefaultValue("DECIMAL(4, 2)", "10.01", new BigDecimal("10.01"));
        checkInsertDefaultValue("CHAR(2)", "'10'", "10");
        checkInsertDefaultValue("VARCHAR", "'10'", "10");
        checkInsertDefaultValue("VARCHAR NOT NULL", "'10'", "10");
        checkInsertDefaultValue("VARCHAR(2)", "'10'", "10");
        checkInsertDefaultValue("DATE", "DATE '2021-01-01'", Date.valueOf("2021-01-01"));
        checkInsertDefaultValue("TIME", "TIME '01:01:01'", Time.valueOf("01:01:01"));
        checkInsertDefaultValue("TIMESTAMP", "TIMESTAMP '2021-01-01 01:01:01'", Timestamp.valueOf("2021-01-01 01:01:01"));
        checkInsertDefaultValue("BINARY(3)", "x'010203'", new byte[] {1, 2, 3});
    }

    /** */
    private void checkInsertDefaultValue(String sqlType, String sqlVal, Object expectedVal) {
        crossCheck(
            stmt -> execute(stmt, "CREATE TABLE test (id INT PRIMARY KEY, val " + sqlType + " DEFAULT " + sqlVal + ")"),
            stmt -> {
                try {
                    execute(stmt, "INSERT INTO test (id) VALUES (0)");

                    List<List<Object>> res = executeQuery(stmt, "SELECT val FROM test");

                    if (expectedVal.getClass().isArray())
                        assertTrue(Objects.deepEquals(expectedVal, res.get(0).get(0)));
                    else
                        assertEquals(expectedVal, res.get(0).get(0));
                }
                finally {
                    execute(stmt, "DROP TABLE IF EXISTS test");
                }
            }
        );
    }

    /** */
    private void execute(Statement stmt, String sql) {
        try {
            stmt.execute(sql);
        }
        catch (SQLException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /** */
    private List<List<Object>> executeQuery(Statement stmt, String sql) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            List<List<Object>> res = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();

                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
                    row.add(rs.getObject(i));

                res.add(row);
            }

            return res;
        }
        catch (SQLException e) {
            throw new IgniteException(e.getMessage(), e);
        }
    }

    /** */
    private void crossCheck(Consumer<Statement> consumer1, Consumer<Statement> consumer2) {
        // Execute consumer1 on non-experimental engine, consumer2 in experimental engine.
        consumer1.accept(stmts[0]);
        consumer2.accept(stmts[1]);
        // Execute consumer1 on non-experimental engine, consumer2 in experimental engine.
        consumer1.accept(stmts[1]);
        consumer2.accept(stmts[0]);
    }
}
