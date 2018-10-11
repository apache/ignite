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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.concurrent.Callable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.sql.Connection.TRANSACTION_REPEATABLE_READ;
import static java.sql.Connection.TRANSACTION_SERIALIZABLE;

/**
 * Connection test.
 */
@SuppressWarnings("ThrowableNotThrown")
public class JdbcThinConnectionMvccEnabledSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** {@inheritDoc} */
    @SuppressWarnings({"deprecation", "unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME).setNearConfiguration(null));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new BinaryMarshaller());
        cfg.setGridLogger(new GridStringLogger());

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(@NotNull String name) {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(name);
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }


    /**
     * @throws Exception If failed.
     */
    public void testMetadataDefaults() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertEquals(TRANSACTION_REPEATABLE_READ, meta.getDefaultTransactionIsolation());
            assertTrue(meta.supportsTransactions());

            assertFalse(meta.supportsTransactionIsolationLevel(TRANSACTION_NONE));
            assertFalse(meta.supportsTransactionIsolationLevel(TRANSACTION_READ_UNCOMMITTED));
            assertFalse(meta.supportsTransactionIsolationLevel(TRANSACTION_READ_COMMITTED));
            assertTrue(meta.supportsTransactionIsolationLevel(TRANSACTION_REPEATABLE_READ));
            assertFalse(meta.supportsTransactionIsolationLevel(TRANSACTION_SERIALIZABLE));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetSetAutoCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertTrue(conn.getMetaData().supportsTransactions());

            assertTrue(conn.getAutoCommit());

            conn.setAutoCommit(false);

            assertFalse(conn.getAutoCommit());

            conn.setAutoCommit(true);

            assertTrue(conn.getAutoCommit());

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setAutoCommit(true);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCommit() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertTrue(conn.getMetaData().supportsTransactions());

            // Should not be called in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.commit();

                        return null;
                    }
                },
                SQLException.class,
                "Transaction cannot be committed explicitly in auto-commit mode"
            );

            conn.setAutoCommit(false);

            conn.commit();

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.commit();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollback() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertTrue(conn.getMetaData().supportsTransactions());

            // Should not be called in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback();

                        return null;
                    }
                },
                SQLException.class,
                "Transaction cannot be rolled back explicitly in auto-commit mode."
            );

            conn.setAutoCommit(false);

            conn.rollback();

            conn.close();

            // Exception when called on closed connection
            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.rollback();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetSavepoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assert !conn.getMetaData().supportsSavepoints();

            // Disallowed in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint();

                        return null;
                    }
                },
                SQLException.class,
                "Savepoint cannot be set in auto-commit mode"
            );

            conn.setAutoCommit(false);

            // Unsupported
            checkNotSupported(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setSavepoint();
                }
            });

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setSavepoint();
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetSavepointName() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assert !conn.getMetaData().supportsSavepoints();

            // Invalid arg
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint(null);

                        return null;
                    }
                },
                SQLException.class,
                "Savepoint name cannot be null"
            );

            final String name = "savepoint";

            // Disallowed in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.setSavepoint(name);

                        return null;
                    }
                },
                SQLException.class,
                "Savepoint cannot be set in auto-commit mode"
            );

            conn.setAutoCommit(false);

            // Unsupported
            checkNotSupported(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setSavepoint(name);
                }
            });

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.setSavepoint(name);
                }
            });
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRollbackSavePoint() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assert !conn.getMetaData().supportsSavepoints();

            // Invalid arg
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback(null);

                        return null;
                    }
                },
                SQLException.class,
                "Invalid savepoint"
            );

            final Savepoint savepoint = getFakeSavepoint();

            // Disallowed in auto-commit mode
            GridTestUtils.assertThrows(log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        conn.rollback(savepoint);

                        return null;
                    }
                },
                SQLException.class,
                "Auto-commit mode"
            );

            conn.setAutoCommit(false);

            // Unsupported
            checkNotSupported(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.rollback(savepoint);
                }
            });

            conn.close();

            checkConnectionClosed(new RunnableX() {
                @Override public void run() throws Exception {
                    conn.rollback(savepoint);
                }
            });
        }
    }

    /**
     * @return Savepoint.
     */
    private Savepoint getFakeSavepoint() {
        return new Savepoint() {
            @Override public int getSavepointId() throws SQLException {
                return 100;
            }

            @Override public String getSavepointName() {
                return "savepoint";
            }
        };
    }
}