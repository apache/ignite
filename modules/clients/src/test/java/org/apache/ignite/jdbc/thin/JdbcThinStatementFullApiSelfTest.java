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
import java.sql.Driver;
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
import org.apache.ignite.configuration.OdbcConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 * Statement test
 */
public class JdbcThinStatementFullApiSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL prefix. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setOdbcConfiguration(new OdbcConfiguration());

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(@NotNull String name) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);

        cfg.setSqlFunctionClasses(getClass());

        cfg.setIndexedTypes(Integer.class, Test.class);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        try {
            Driver drv = DriverManager.getDriver(URL);

            if (drv != null)
                DriverManager.deregisterDriver(drv);
        } catch (SQLException ignored) {
            // No-op.
        }

        startGridsMultiThreaded(2);

        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

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
    public void testExecuteQuery() throws Exception {
        final String sqlText = "select * from test";

        try (ResultSet rs = stmt.executeQuery(sqlText)) {
            assertNotNull(rs);

            assertTrue(rs.next());

            int val = rs.getInt(1);

            assertTrue(1 >= val && val <= 10);
        }

        stmt.close();

        // Call on a closed statement
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.executeQuery(sqlText);
                }
            },
            SQLException.class,
            "Statement is closed"
        );
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
    public void testExecuteQueryMultipleResultSets() throws Exception {
        if (!conn.getMetaData().supportsMultipleResultSets())
            return;

        final String sqlText = "select * from test; select * from test";

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.executeQuery(sqlText);
                }
            },
            SQLException.class,
            "Multiple result sets"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testExecuteUpdate() throws Exception {
        final String sqlText = "update test set val=1 where _key=1";

        assertEquals(1, stmt.executeUpdate(sqlText));

        stmt.close();

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.executeUpdate(sqlText);
                }
            },
            SQLException.class,
            "Statement is closed"
        );
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
            "The query is not DML"
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
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.getMaxFieldSize();
                }
            },
            SQLException.class,
            "Statement is closed"
        );

        // Call on a closed statement
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setMaxFieldSize(100);

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );
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
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.getMaxRows();
                }
            },
            SQLException.class,
            "Statement is closed"
        );

        // Call on a closed statement
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setMaxRows(maxRows);

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );
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

        // Call on a closed statement
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setEscapeProcessing(true);

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );
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
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return stmt.getQueryTimeout();
                }
            },
            SQLException.class,
            "Statement is closed"
        );

        // Call on a closed statement
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setQueryTimeout(timeout);

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );
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

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.getQueryTimeout();

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setQueryTimeout(10);

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testWarningsOnClosedStatement() throws Exception {
        stmt.clearWarnings();

        assert stmt.getWarnings() == null;

        stmt.close();

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.getWarnings();

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.clearWarnings();

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );
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

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setCursorName("test");

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetMoreResults() throws Exception {
        assert !stmt.getMoreResults();

        stmt.execute("select 1");

        ResultSet rs = stmt.getResultSet();

        assert !stmt.getMoreResults();

        assert rs.isClosed();

        stmt.close();

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.getMoreResults();

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetMoreResults1() throws Exception {
        assert !stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
        assert !stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
        assert !stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS);

        stmt.execute("select 1");

        ResultSet rs = stmt.getResultSet();

        assert !stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);

        assert !rs.isClosed();

        assert !stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS);

        assert rs.isClosed();

        stmt.close();

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testBatch() throws Exception {
        if (!conn.getMetaData().supportsBatchUpdates()) {
            checkNotSupported(new RunnableX() {
                @Override public void run() throws Exception {
                    stmt.addBatch("");
                }
            });

            checkNotSupported(new RunnableX() {
                @Override public void run() throws Exception {
                    stmt.clearBatch();
                }
            });

            checkNotSupported(new RunnableX() {
                @Override public void run() throws Exception {
                    stmt.executeBatch();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFetchDirection() throws Exception {
        assert stmt.getFetchDirection() == ResultSet.FETCH_FORWARD;

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setFetchDirection(-1);

                    return null;
                }
            },
            SQLException.class,
            "Invalid fetch direction"
        );

        stmt.close();

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.setFetchDirection(-1);

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );

        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    stmt.getFetchDirection();

                    return null;
                }
            },
            SQLException.class,
            "Statement is closed"
        );
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

        if(!conn.getMetaData().supportsGetGeneratedKeys()) {
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
    }

    /**
     * @throws Exception If failed.
     */
    public void testCloseOnCompletion() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5435");

        assert !stmt.isCloseOnCompletion() : "Default value of CloseOnCompletion is invalid";

        stmt.execute("select 1");

        stmt.closeOnCompletion();

        assert stmt.isCloseOnCompletion();

        stmt.getResultSet().close();

        assert stmt.isClosed() : "Must be closed on complete";
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

    /** */
    private void fillCache() {
        IgniteCache<Integer, Test> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        int count = 10;

        for (int i = 1; i <= count; i++)
            cache.put(i, new Test(i));
    }

    /** */
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
}
