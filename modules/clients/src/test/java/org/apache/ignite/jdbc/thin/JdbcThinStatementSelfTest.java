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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Statement test.
 */
@SuppressWarnings({"ThrowableNotThrown", "ThrowableResultOfMethodCallIgnored"})
public class JdbcThinStatementSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** SQL query. */
    private static final String SQL = "select * from Person where age > 30";

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(
            String.class, Person.class,
            Integer.class, Test.class
        );

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);

        fillCache();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(URL);

        conn.setSchema(DEFAULT_CACHE_NAME);

        stmt = conn.createStatement();

        assert stmt != null;
        assert !stmt.isClosed();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (stmt != null && !stmt.isClosed()) {
            stmt.close();

            assert stmt.isClosed();
        }

        conn.close();

        assert stmt.isClosed();
        assert conn.isClosed();
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteQuery0() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assert "Joe".equals(rs.getString("firstName"));
                assert "Black".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 35;
            }
            else if (id == 3) {
                assert "Mike".equals(rs.getString("firstName"));
                assert "Green".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 40;
            }
            else
                assert false : "Wrong ID: " + id;

            cnt++;
        }

        assert cnt == 2;
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteQuery1() throws Exception {
        final String sqlText = "select val from test";

        try (ResultSet rs = stmt.executeQuery(sqlText)) {
            assertNotNull(rs);

            assertTrue(rs.next());

            int val = rs.getInt(1);

            assertTrue("Invalid val: " + val, val >= 1 && val <= 10);
        }

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.executeQuery(sqlText);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecute() throws Exception {
        assert stmt.execute(SQL);

        assert stmt.getUpdateCount() == -1 : "Update count must be -1 for SELECT query";

        ResultSet rs = stmt.getResultSet();

        assert rs != null;

        assert stmt.getResultSet() == null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assert "Joe".equals(rs.getString("firstName"));
                assert "Black".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 35;
            }
            else if (id == 3) {
                assert "Mike".equals(rs.getString("firstName"));
                assert "Green".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 40;
            }
            else
                assert false : "Wrong ID: " + id;

            cnt++;
        }

        assert cnt == 2;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxRows() throws Exception {
        stmt.setMaxRows(1);

        assert stmt.getMaxRows() == 1;

        ResultSet rs = stmt.executeQuery(SQL);

        assert rs != null;

        int cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assert "Joe".equals(rs.getString("firstName"));
                assert "Black".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 35;
            }
            else if (id == 3) {
                assert "Mike".equals(rs.getString("firstName"));
                assert "Green".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 40;
            }
            else
                assert false : "Wrong ID: " + id;

            cnt++;
        }

        assert cnt == 1;

        stmt.setMaxRows(0);

        rs = stmt.executeQuery(SQL);

        assert rs != null;

        cnt = 0;

        while (rs.next()) {
            int id = rs.getInt("id");

            if (id == 2) {
                assert "Joe".equals(rs.getString("firstName"));
                assert "Black".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 35;
            }
            else if (id == 3) {
                assert "Mike".equals(rs.getString("firstName"));
                assert "Green".equals(rs.getString("lastName"));
                assert rs.getInt("age") == 40;
            }
            else
                assert false : "Wrong ID: " + id;

            cnt++;
        }

        assert cnt == 2;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCloseResultSet0() throws Exception {
        ResultSet rs0 = stmt.executeQuery(SQL);
        ResultSet rs1 = stmt.executeQuery(SQL);
        ResultSet rs2 = stmt.executeQuery(SQL);

        assert rs0.isClosed() : "ResultSet must be implicitly closed after re-execute statement";
        assert rs1.isClosed() : "ResultSet must be implicitly closed after re-execute statement";

        assert !rs2.isClosed() : "Last result set must be available";

        stmt.close();

        assert rs2.isClosed() : "ResultSet must be explicitly closed after close statement";
    }

    /**
     * @throws Exception If failed.
     */
    public void testCloseResultSet1() throws Exception {
        stmt.execute(SQL);

        ResultSet rs = stmt.getResultSet();

        stmt.close();

        assert rs.isClosed() : "ResultSet must be explicitly closed after close statement";
    }

    /**
     * @throws Exception If failed.
     */
    public void testCloseResultSetByConnectionClose() throws Exception {
        ResultSet rs = stmt.executeQuery(SQL);

        conn.close();

        assert stmt.isClosed() : "Statement must be implicitly closed after close connection";
        assert rs.isClosed() : "ResultSet must be implicitly closed after close connection";
    }

    /**
     * @throws Exception If failed.
     */
    public void testCloseOnCompletionAfterQuery() throws Exception {
        assert !stmt.isCloseOnCompletion() : "Invalid default closeOnCompletion";

        ResultSet rs0 = stmt.executeQuery(SQL);

        ResultSet rs1 = stmt.executeQuery(SQL);

        assert rs0.isClosed() : "Result set must be closed implicitly";

        assert !stmt.isClosed() : "Statement must not be closed";

        rs1.close();

        assert !stmt.isClosed() : "Statement must not be closed";

        ResultSet rs2 = stmt.executeQuery(SQL);

        stmt.closeOnCompletion();

        assert stmt.isCloseOnCompletion() : "Invalid closeOnCompletion";

        rs2.close();

        assert stmt.isClosed() : "Statement must be closed";
    }

    /**
     * @throws Exception If failed.
     */
    public void testCloseOnCompletionBeforeQuery() throws Exception {
        assert !stmt.isCloseOnCompletion() : "Invalid default closeOnCompletion";

        ResultSet rs0 = stmt.executeQuery(SQL);

        ResultSet rs1 = stmt.executeQuery(SQL);

        assert rs0.isClosed() : "Result set must be closed implicitly";

        assert !stmt.isClosed() : "Statement must not be closed";

        rs1.close();

        assert !stmt.isClosed() : "Statement must not be closed";

        stmt.closeOnCompletion();

        ResultSet rs2 = stmt.executeQuery(SQL);

        assert stmt.isCloseOnCompletion() : "Invalid closeOnCompletion";

        rs2.close();

        assert stmt.isClosed() : "Statement must be closed";
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteQueryTimeout() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5438");

        final String sqlText = "select sleep_func(3)";

        stmt.setQueryTimeout(1);

        // Timeout
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.executeQuery(sqlText);
                }
            },
            SQLTimeoutException.class,
            "Timeout"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteQueryMultipleOnlyResultSets() throws Exception {
        assert conn.getMetaData().supportsMultipleResultSets();

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder();

        for (int i = 0; i < stmtCnt; ++i)
            sql.append("select ").append(i).append("; ");

        assert stmt.execute(sql.toString());

        for (int i = 0; i < stmtCnt; ++i) {
            assert stmt.getMoreResults();

            ResultSet rs = stmt.getResultSet();

            assert rs.next();
            assert rs.getInt(1) == i;
            assert !rs.next();
        }

        assert !stmt.getMoreResults();
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteQueryMultipleOnlyDml() throws Exception {
        conn.setSchema(null);

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder("drop table if exists test; create table test(ID int primary key, NAME varchar(20)); ");

        for (int i = 0; i < stmtCnt; ++i)
            sql.append("insert into test (ID, NAME) values (" + i + ", 'name_" + i +"'); ");

        assert !stmt.execute(sql.toString());

        // DROP TABLE statement
        assert stmt.getResultSet() == null;
        assert stmt.getUpdateCount() == 0;

        // CREATE TABLE statement
        assert stmt.getResultSet() == null;
        assert stmt.getUpdateCount() == 0;

        for (int i = 0; i < stmtCnt; ++i) {
            assert stmt.getMoreResults();

            assert stmt.getResultSet() == null;
            assert stmt.getUpdateCount() == 1;
        }

        assert !stmt.getMoreResults();
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteQueryMultipleMixed() throws Exception {
        conn.setSchema(null);

        int stmtCnt = 10;

        StringBuilder sql = new StringBuilder("drop table if exists test; create table test(ID int primary key, NAME varchar(20)); ");

        for (int i = 0; i < stmtCnt; ++i) {
            if (i % 2 == 0)
                sql.append(" insert into test (ID, NAME) values (" + i + ", 'name_" + i + "'); ");
            else
                sql.append(" select * from test where id < " + i + "; ");
        }

        assert !stmt.execute(sql.toString());

        // DROP TABLE statement
        assert stmt.getResultSet() == null;
        assert stmt.getUpdateCount() == 0;

        // CREATE TABLE statement
        assert stmt.getResultSet() == null;
        assert stmt.getUpdateCount() == 0;

        boolean notEmptyResult = false;

        for (int i = 0; i < stmtCnt; ++i) {
            assert stmt.getMoreResults();

            if (i % 2 == 0) {
                assert stmt.getResultSet() == null;
                assert stmt.getUpdateCount() == 1;
            }
            else {
                assert stmt.getUpdateCount() == -1;

                ResultSet rs = stmt.getResultSet();

                int rowsCnt = 0;

                while(rs.next())
                    rowsCnt++;

                assert rowsCnt <= (i + 1) / 2;

                if (rowsCnt == (i + 1) / 2)
                    notEmptyResult = true;
            }
        }

        assert notEmptyResult;

        assert !stmt.getMoreResults();
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteUpdate() throws Exception {
        final String sqlText = "update test set val=1 where _key=1";

        assertEquals(1, stmt.executeUpdate(sqlText));

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.executeUpdate(sqlText);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteUpdateProducesResultSet() throws Exception {
        final String sqlText = "select * from test";

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.executeUpdate(sqlText);
                }
            },
            SQLException.class,
            "Given statement type does not match that declared by JDBC driver"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteUpdateTimeout() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5438");

        final String sqlText = "update test set val=1 where _key=sleep_func(3)";

        stmt.setQueryTimeout(1);

        // Timeout
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.executeUpdate(sqlText);
                }
            },
            SQLTimeoutException.class,
            "Timeout"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testClose() throws Exception {
        String sqlText = "select * from test";

        ResultSet rs = stmt.executeQuery(sqlText);

        assertTrue(rs.next());
        assertFalse(rs.isClosed());

        assertFalse(stmt.isClosed());

        stmt.close();
        stmt.close(); // Closing closed is ok

        assertTrue(stmt.isClosed());

        // Current result set must be closed
        assertTrue(rs.isClosed());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetMaxFieldSizeUnsupported() throws Exception {
        assertEquals(0, stmt.getMaxFieldSize());

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setMaxFieldSize(100);

                    return null;
                }
            },
            SQLFeatureNotSupportedException.class,
            "Field size limitation is not supported"
        );

        assertEquals(0, stmt.getMaxFieldSize());

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.getMaxFieldSize();
            }
        });

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.setMaxFieldSize(100);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetMaxRows() throws Exception {
        assertEquals(0, stmt.getMaxRows());

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setMaxRows(-1);

                    return null;
                }
            },
            SQLException.class,
            "Invalid max rows value"
        );

        assertEquals(0, stmt.getMaxRows());

        final int maxRows = 1;

        stmt.setMaxRows(maxRows);

        assertEquals(maxRows, stmt.getMaxRows());

        String sqlText = "select * from test";

        ResultSet rs = stmt.executeQuery(sqlText);

        assertTrue(rs.next());
        assertFalse(rs.next()); //max rows reached

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.getMaxRows();
            }
        });

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.setMaxRows(maxRows);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetEscapeProcessing() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5440");

        stmt.setEscapeProcessing(false);

        final String sqlText = "select {fn CONVERT(1, SQL_BOOLEAN)}";

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.executeQuery(sqlText);
                }
            },
            SQLException.class,
            "Failed to parse"
        );

        ResultSet rs = stmt.executeQuery(sqlText);

        assertTrue(rs.next());

        assertEquals(true, rs.getBoolean(1));

        stmt.setEscapeProcessing(true);

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.setEscapeProcessing(true);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetQueryTimeout() throws Exception {
        assertEquals(0, stmt.getQueryTimeout());

        // Invalid argument
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setQueryTimeout(-1);

                    return null;
                }
            },
            SQLException.class,
            "Invalid timeout value"
        );

        assertEquals(0, stmt.getQueryTimeout());

        final int timeout = 3;

        stmt.setQueryTimeout(timeout);

        assertEquals(timeout, stmt.getQueryTimeout());

        stmt.close();

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.getQueryTimeout();
            }
        });

        // Call on a closed statement
        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.setQueryTimeout(timeout);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxFieldSize() throws Exception {
        assert stmt.getMaxFieldSize() >= 0;

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setMaxFieldSize(-1);

                    return null;
                }
            },
            SQLException.class,
            "Invalid field limit"
        );

        checkNotSupported(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.setMaxFieldSize(100);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryTimeout() throws Exception {
        assert stmt.getQueryTimeout() == 0 : "Default timeout invalid: " + stmt.getQueryTimeout();

        stmt.setQueryTimeout(10);

        assert stmt.getQueryTimeout() == 10;

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.getQueryTimeout();
            }
        });

        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.setQueryTimeout(10);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testWarningsOnClosedStatement() throws Exception {
        stmt.clearWarnings();

        assert stmt.getWarnings() == null;

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.getWarnings();
            }
        });

        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.clearWarnings();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCursorName() throws Exception {
        checkNotSupported(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.setCursorName("test");
            }
        });

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.setCursorName("test");
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetMoreResults() throws Exception {
        assert !stmt.getMoreResults();

        stmt.execute("select 1; ");

        ResultSet rs = stmt.getResultSet();

        assert !stmt.getMoreResults();

        assert stmt.getResultSet() == null;

        assert rs.isClosed();

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.getMoreResults();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetMoreResults1() throws Exception {
        assert !stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
        assert !stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
        assert !stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS);

        stmt.execute("select 1; ");

        ResultSet rs = stmt.getResultSet();

        assert !stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);

        assert !rs.isClosed();

        assert !stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS);

        assert rs.isClosed();

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBatchEmpty() throws Exception {
        assert conn.getMetaData().supportsBatchUpdates();

        stmt.addBatch("");
        stmt.clearBatch();

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.executeBatch();

                    return null;
                }
            },
            SQLException.class,
            "Batch is empty"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testFetchDirection() throws Exception {
        assert stmt.getFetchDirection() == ResultSet.FETCH_FORWARD;

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setFetchDirection(ResultSet.FETCH_REVERSE);

                    return null;
                }
            },
            SQLFeatureNotSupportedException.class,
            "Only forward direction is supported."
        );

        stmt.close();

        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.setFetchDirection(-1);
            }
        });

        checkStatementClosed(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.getFetchDirection();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testAutogenerated() throws Exception {
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.executeUpdate("select 1", -1);

                    return null;
                }
            },
            SQLException.class,
            "Invalid autoGeneratedKeys value");

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.execute("select 1", -1);

                    return null;
                }
            },
            SQLException.class,
            "Invalid autoGeneratedKeys value");

        assert !conn.getMetaData().supportsGetGeneratedKeys();

        checkNotSupported(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.getGeneratedKeys();
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.executeUpdate("select 1", Statement.RETURN_GENERATED_KEYS);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.executeUpdate("select 1", new int[] {1, 2});
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.executeUpdate("select 1", new String[] {"a", "b"});
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.execute("select 1", Statement.RETURN_GENERATED_KEYS);
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.execute("select 1", new int[] {1, 2});
            }
        });

        checkNotSupported(new RunnableX() {
            @Override public void run() throws Exception {
                stmt.execute("select 1", new String[] {"a", "b"});
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancel() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5439");

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.execute("select sleep_func(3)");

                    return null;
                }
            },
            SQLException.class,
            "The query is canceled");

        IgniteInternalFuture f = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    stmt.cancel();
                }
                catch (SQLException e) {
                    log.error("Unexpected exception", e);

                    fail("Unexpected exception.");
                }
            }
        });

        f.get();

        stmt.close();

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.cancel();

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed");
    }

    /**
     * @throws Exception If failed.
     */
    public void testStatementTypeMismatchSelectForCachedQuery() throws Exception {
        // Put query to cache.
        stmt.executeQuery("select 1;");

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.executeUpdate("select 1;");

                    return null;
                }
            },
            SQLException.class,
            "Given statement type does not match that declared by JDBC driver");

        assert stmt.getResultSet() == null : "Not results expected. Last statement is executed with exception";
    }

    /**
     * @throws Exception If failed.
     */
    public void testStatementTypeMismatchUpdate() throws Exception {
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.executeQuery("update test set val=28 where _key=1");

                    return null;
                }
            },
            SQLException.class,
            "Given statement type does not match that declared by JDBC driver");

        ResultSet rs = stmt.executeQuery("select val from test where _key=1");

        boolean next = rs.next();

        assert next;

        assert rs.getInt(1) == 1 : "The data must not be updated. " +
            "Because update statement is executed via 'executeQuery' method." +
            " Data [val=" + rs.getInt(1) + ']';
    }

    /** */
    private void fillCache() {
        IgniteCache<String, Person> cachePerson = grid(0).cache(DEFAULT_CACHE_NAME);

        assert cachePerson != null;

        cachePerson.put("p1", new Person(1, "John", "White", 25));
        cachePerson.put("p2", new Person(2, "Joe", "Black", 35));
        cachePerson.put("p3", new Person(3, "Mike", "Green", 40));

        IgniteCache<Integer, Test> cacheTest = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 1; i <= 10; i++)
            cacheTest.put(i, new Test(i));
    }

    /** */
    @SuppressWarnings("unused")
    public static class Test {
        @QuerySqlField
        private int val;

        /**
         * @param val Value.
         */
        public Test(int val) {
            this.val = val;
        }
    }

    /**
     *
     * @param v seconds to sleep
     * @return passed value
     */
    @SuppressWarnings("unused")
    @QuerySqlFunction
    public static int sleep_func(int v) {
        try {
            Thread.sleep(v * 1000);
        }
        catch (InterruptedException ignored) {
            // No-op
        }
        return v;
    }

    /**
     * Person.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Person implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** First name. */
        @QuerySqlField
        private final String firstName;

        /** Last name. */
        @QuerySqlField
        private final String lastName;

        /** Age. */
        @QuerySqlField
        private final int age;

        /**
         * @param id ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param age Age.
         */
        private Person(int id, String firstName, String lastName, int age) {
            assert !F.isEmpty(firstName);
            assert !F.isEmpty(lastName);
            assert age > 0;

            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }
    }
}