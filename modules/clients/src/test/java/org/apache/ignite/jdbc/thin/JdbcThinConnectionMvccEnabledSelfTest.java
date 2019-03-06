/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

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
    /** */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** {@inheritDoc} */
    @SuppressWarnings({"deprecation", "unchecked"})
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME).setNearConfiguration(null));

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

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
    @Test
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
