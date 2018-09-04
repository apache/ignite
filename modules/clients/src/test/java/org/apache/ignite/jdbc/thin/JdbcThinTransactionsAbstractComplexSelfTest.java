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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test to check various transactional scenarios.
 */
public abstract class JdbcThinTransactionsAbstractComplexSelfTest extends JdbcThinAbstractSelfTest {
    /** Client node index. */
    final static int CLI_IDX = 1;

    /**
     * Closure to perform ordinary delete after repeatable read.
     */
    private final IgniteInClosure<Connection> afterReadDel = new IgniteInClosure<Connection>() {
        @Override public void apply(Connection conn) {
            execute(conn, "DELETE FROM \"Person\".Person where firstname = 'John'");
        }
    };

    /**
     * Closure to perform fast delete after repeatable read.
     */
    private final IgniteInClosure<Connection> afterReadFastDel = new IgniteInClosure<Connection>() {
        @Override public void apply(Connection conn) {
            execute(conn, "DELETE FROM \"Person\".Person where id = 1");
        }
    };

    /**
     * Closure to perform ordinary update after repeatable read.
     */
    private final IgniteInClosure<Connection> afterReadUpdate = new IgniteInClosure<Connection>() {
        @Override public void apply(Connection conn) {
            execute(conn, "UPDATE \"Person\".Person set firstname = 'Joe' where firstname = 'John'");
        }
    };

    /**
     * Closure to perform ordinary delete and rollback after repeatable read.
     */
    private final IgniteInClosure<Connection> afterReadDelAndRollback = new IgniteInClosure<Connection>() {
        @Override public void apply(Connection conn) {
            execute(conn, "DELETE FROM \"Person\".Person where firstname = 'John'");

            rollback(conn);
        }
    };

    /**
     * Closure to perform fast delete after repeatable read.
     */
    private final IgniteInClosure<Connection> afterReadFastDelAndRollback = new IgniteInClosure<Connection>() {
        @Override public void apply(Connection conn) {
            execute(conn, "DELETE FROM \"Person\".Person where id = 1");

            rollback(conn);
        }
    };

    /**
     * Closure to perform ordinary update and rollback after repeatable read.
     */
    private final IgniteInClosure<Connection> afterReadUpdateAndRollback = new IgniteInClosure<Connection>() {
        @Override public void apply(Connection conn) {
            execute(conn, "UPDATE \"Person\".Person set firstname = 'Joe' where firstname = 'John'");

            rollback(conn);
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String testIgniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(testIgniteInstanceName);

        cfg.setMvccEnabled(true);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>("Person");

        ccfg.setIndexedTypes(Integer.class, Person.class);

        ccfg.getQueryEntities().iterator().next().setKeyFieldName("id");

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        ccfg.setCacheMode(CacheMode.PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        // Let the node with index 1 be client node.
        cfg.setClientMode(F.eq(testIgniteInstanceName, getTestIgniteInstanceName(CLI_IDX)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        execute("ALTER TABLE \"Person\".person add if not exists cityid int");

        execute("ALTER TABLE \"Person\".person add if not exists companyid int");

        execute("CREATE TABLE City (id int primary key, name varchar, population int) WITH " +
            "\"atomicity=transactional,template=partitioned,backups=3,cache_name=City\"");

        execute("CREATE TABLE Company (id int, \"cityid\" int, name varchar, primary key (id, \"cityid\")) WITH " +
            "\"atomicity=transactional,template=partitioned,backups=1,wrap_value=false,affinity_key=cityid," +
            "cache_name=Company\"");

        execute("CREATE TABLE Product (id int primary key, name varchar, companyid int) WITH " +
            "\"atomicity=transactional,template=partitioned,backups=2,cache_name=Product\"");

        execute("CREATE INDEX IF NOT EXISTS prodidx ON Product(companyid)");

        execute("CREATE INDEX IF NOT EXISTS persidx ON \"Person\".person(cityid)");

        insertPerson(1, "John", "Smith", 1, 1);

        insertPerson(2, "Mike", "Johns", 1, 2);

        insertPerson(3, "Sam", "Jules", 2, 2);

        insertPerson(4, "Alex", "Pope", 2, 3);

        insertPerson(5, "Peter", "Williams", 2, 3);

        insertCity(1, "Los Angeles", 5000);

        insertCity(2, "Seattle", 1500);

        insertCity(3, "New York", 12000);

        insertCity(4, "Cupertino", 400);

        insertCompany(1, "Microsoft", 2);

        insertCompany(2, "Google", 3);

        insertCompany(3, "Facebook", 1);

        insertCompany(4, "Uber", 1);

        insertCompany(5, "Apple", 4);

        insertProduct(1, "Search", 2);

        insertProduct(2, "Windows", 1);

        insertProduct(3, "Mac", 5);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        startGrid(1);

        startGrid(2);

        startGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        execute("DELETE FROM \"Person\".Person");

        execute("DROP TABLE City");

        execute("DROP TABLE Company");

        execute("DROP TABLE Product");

        super.afterTest();
    }

    /**
     *
     */
    public void testSingleDmlStatement() throws SQLException {
        insertPerson(6, "John", "Doe", 2, 2);

        assertEquals(Collections.singletonList(l(6, "John", "Doe", 2, 2)),
            execute("SELECT * FROM \"Person\".Person where id = 6"));
    }

    /**
     *
     */
    public void testMultipleDmlStatements() throws SQLException {
        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection conn) {
                insertPerson(conn, 6, "John", "Doe", 2, 2);

                // https://issues.apache.org/jira/browse/IGNITE-6938 - we can only see results of
                // UPDATE of what we have not inserted ourselves.
                execute(conn, "UPDATE \"Person\".person SET lastname = 'Jameson' where lastname = 'Jules'");

                execute(conn, "DELETE FROM \"Person\".person where id = 5");
            }
        });

        assertEquals(l(
            l(3, "Sam", "Jameson", 2, 2),
            l(6, "John", "Doe", 2, 2)
        ), execute("SELECT * FROM \"Person\".Person where id = 3 or id >= 5 order by id"));
    }

    /**
     *
     */
    public void testBatchDmlStatements() throws SQLException {
        doBatchedInsert();

        assertEquals(l(
            l(6, "John", "Doe", 2, 2),
            l(7, "Mary", "Lee", 1, 3)
        ), execute("SELECT * FROM \"Person\".Person where id > 5 order by id"));
    }

    /**
     *
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testBatchDmlStatementsIntermediateFailure() throws SQLException {
        insertPerson(6, "John", "Doe", 2, 2);

        IgniteException e = (IgniteException)GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                doBatchedInsert();

                return null;
            }
        }, IgniteException.class, "Duplicate key during INSERT [key=KeyCacheObjectImpl " +
            "[part=6, val=6, hasValBytes=true]]");

        assertTrue(e.getCause() instanceof BatchUpdateException);

        assertTrue(e.getCause().getMessage().contains("Duplicate key during INSERT [key=KeyCacheObjectImpl " +
            "[part=6, val=6, hasValBytes=true]]"));

        // First we insert id 7, then 6. Still, 7 is not in the cache as long as the whole batch has failed inside tx.
        assertEquals(Collections.emptyList(), execute("SELECT * FROM \"Person\".Person where id > 6 order by id"));
    }

    /**
     *
     */
    private void doBatchedInsert() throws SQLException {
        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection conn) {
                try {
                    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO \"Person\".person " +
                        "(id, firstName, lastName, cityId, companyId) values (?, ?, ?, ?, ?)")) {
                        ps.setInt(1, 7);

                        ps.setString(2, "Mary");

                        ps.setString(3, "Lee");

                        ps.setInt(4, 1);

                        ps.setInt(5, 3);

                        ps.addBatch();

                        ps.setInt(1, 6);

                        ps.setString(2, "John");

                        ps.setString(3, "Doe");

                        ps.setInt(4, 2);

                        ps.setInt(5, 2);

                        ps.addBatch();

                        ps.executeBatch();
                    }
                }
                catch (SQLException e) {
                    throw new IgniteException(e);
                }
            }
        });
    }

    /**
     *
     */
    public void testInsertAndQueryMultipleCaches() throws SQLException {
        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection conn) {
                insertCity(conn, 5, "St Petersburg", 6000);

                insertCompany(conn, 6, "VK", 5);

                insertPerson(conn, 6, "Peter", "Sergeev", 5, 6);
            }
        });

        try (Connection c = connect("distributedJoins=true")) {
            assertEquals(l(l(5, "St Petersburg", 6000, 6, 5, "VK", 6, "Peter", "Sergeev", 5, 6)),
                execute(c, "SELECT * FROM City left join Company on City.id = Company.\"cityid\" " +
                    "left join \"Person\".Person p on City.id = p.cityid WHERE p.id = 6 or company.id = 6"));
        }
    }

    /**
     *
     */
    public void testColocatedJoinSelectAndInsertInTransaction() throws SQLException {
        // We'd like to put some Google into cities with over 1K population which don't have it yet
        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection conn) {
                List<Integer> ids = flat(execute(conn, "SELECT distinct City.id from City left join Company c on " +
                    "City.id = c.\"cityid\" where population >= 1000 and c.name <> 'Google' order by City.id"));

                assertEqualsCollections(l(1, 2), ids);

                int i = 5;

                for (int l : ids)
                    insertCompany(conn, ++i, "Google", l);
            }
        });

        assertEqualsCollections(l("Los Angeles", "Seattle", "New York"), flat(execute("SELECT City.name from City " +
            "left join Company c on city.id = c.\"cityid\" WHERE c.name = 'Google' order by City.id")));
    }

    /**
     *
     */
    public void testDistributedJoinSelectAndInsertInTransaction() throws SQLException {
        try (Connection c = connect("distributedJoins=true")) {
            // We'd like to put some Google into cities with over 1K population which don't have it yet
            executeInTransaction(c, new TransactionClosure() {
                @Override public void apply(Connection conn) {
                    List<?> res = flat(execute(conn, "SELECT p.id,p.name,c.id from Company c left join Product p on " +
                        "c.id = p.companyid left join City on city.id = c.\"cityid\" WHERE c.name <> 'Microsoft' " +
                        "and population < 1000"));

                    assertEqualsCollections(l(3, "Mac", 5), res);

                    insertProduct(conn, 4, (String)res.get(1), 1);
                }
            });
        }

        try (Connection c = connect("distributedJoins=true")) {
            assertEqualsCollections(l("Windows", "Mac"), flat(execute(c, "SELECT p.name from Company c left join " +
                "Product p on c.id = p.companyid WHERE c.name = 'Microsoft' order by p.id")));
        }
    }

    /**
     *
     */
    public void testInsertFromExpression() throws SQLException {
        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection conn) {
                execute(conn, "insert into city (id, name, population) values (? + 1, ?, ?)",
                    8, "Moscow", 15000);
            }
        });
    }

    /**
     *
     */
    public void testAutoRollback() throws SQLException {
        try (Connection c = connect()) {
            begin(c);

            insertPerson(c, 6, "John", "Doe", 2, 2);
        }

        // Connection has not hung on close and update has not been applied.
        assertTrue(personCache().query(new SqlFieldsQuery("SELECT * FROM \"Person\".Person WHERE id = 6"))
            .getAll().isEmpty());
    }

    /**
     *
     */
    public void testRepeatableReadWithConcurrentDelete() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "DELETE FROM \"Person\".Person where firstname = 'John'");
            }
        }, null);
    }

    /**
     *
     */
    public void testRepeatableReadWithConcurrentFastDelete() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "DELETE FROM \"Person\".Person where id = 1");
            }
        }, null);
    }

    /**
     *
     */
    public void testRepeatableReadWithConcurrentCacheRemove() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                personCache().remove(1);
            }
        }, null);
    }

    /**
     *
     */
    public void testRepeatableReadAndDeleteWithConcurrentDelete() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "DELETE FROM \"Person\".Person where firstname = 'John'");
            }
        }, afterReadDel);
    }

    /**
     *
     */
    public void testRepeatableReadAndDeleteWithConcurrentFastDelete() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "DELETE FROM \"Person\".Person where id = 1");
            }
        }, afterReadDel);
    }

    /**
     *
     */
    public void testRepeatableReadAndDeleteWithConcurrentCacheRemove() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                personCache().remove(1);
            }
        }, afterReadDel);
    }

    /**
     *
     */
    public void testRepeatableReadAndFastDeleteWithConcurrentDelete() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "DELETE FROM \"Person\".Person where firstname = 'John'");
            }
        }, afterReadFastDel);
    }

    /**
     *
     */
    public void testRepeatableReadAndFastDeleteWithConcurrentFastDelete() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "DELETE FROM \"Person\".Person where id = 1");
            }
        }, afterReadFastDel);
    }

    /**
     *
     */
    public void testRepeatableReadAndFastDeleteWithConcurrentCacheRemove() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                personCache().remove(1);
            }
        }, afterReadFastDel);
    }

    /**
     *
     */
    public void testRepeatableReadAndDeleteWithConcurrentDeleteAndRollback() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "DELETE FROM \"Person\".Person where firstname = 'John'");
            }
        }, afterReadDelAndRollback);
    }

    /**
     *
     */
    public void testRepeatableReadAndDeleteWithConcurrentFastDeleteAndRollback() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "DELETE FROM \"Person\".Person where id = 1");
            }
        }, afterReadDelAndRollback);
    }

    /**
     *
     */
    public void testRepeatableReadAndDeleteWithConcurrentCacheRemoveAndRollback() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                personCache().remove(1);
            }
        }, afterReadDelAndRollback);
    }

    /**
     *
     */
    public void testRepeatableReadAndFastDeleteWithConcurrentDeleteAndRollback() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "DELETE FROM \"Person\".Person where firstname = 'John'");
            }
        }, afterReadFastDelAndRollback);
    }

    /**
     *
     */
    public void testRepeatableReadAndFastDeleteWithConcurrentFastDeleteAndRollback() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "DELETE FROM \"Person\".Person where id = 1");
            }
        }, afterReadFastDelAndRollback);
    }

    /**
     *
     */
    public void testRepeatableReadAndFastDeleteWithConcurrentCacheRemoveAndRollback() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                personCache().remove(1);
            }
        }, afterReadFastDelAndRollback);
    }

    /**
     *
     */
    public void testRepeatableReadWithConcurrentUpdate() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "UPDATE \"Person\".Person SET lastname = 'Fix' where firstname = 'John'");
            }
        }, null);
    }

    /**
     *
     */
    public void testRepeatableReadWithConcurrentCacheReplace() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                Person p = new Person();

                p.id = 1;
                p.firstName = "Luke";
                p.lastName = "Maxwell";

                personCache().replace(1, p);
            }
        }, null);
    }

    /**
     *
     */
    public void testRepeatableReadAndUpdateWithConcurrentUpdate() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "UPDATE \"Person\".Person SET lastname = 'Fix' where firstname = 'John'");
            }
        }, afterReadUpdate);
    }

    /**
     *
     */
    public void testRepeatableReadAndUpdateWithConcurrentCacheReplace() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                Person p = new Person();

                p.id = 1;
                p.firstName = "Luke";
                p.lastName = "Maxwell";

                personCache().replace(1, p);
            }
        }, afterReadUpdate);
    }

    /**
     *
     */
    public void testRepeatableReadAndUpdateWithConcurrentUpdateAndRollback() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                execute(conn, "UPDATE \"Person\".Person SET lastname = 'Fix' where firstname = 'John'");
            }
        }, afterReadUpdateAndRollback);
    }

    /**
     *
     */
    public void testRepeatableReadAndUpdateWithConcurrentCacheReplaceAndRollback() throws Exception {
        doTestRepeatableRead(new IgniteInClosure<Connection>() {
            @Override public void apply(Connection conn) {
                Person p = new Person();

                p.id = 1;
                p.firstName = "Luke";
                p.lastName = "Maxwell";

                personCache().replace(1, p);
            }
        }, afterReadUpdateAndRollback);
    }

    /**
     * Perform repeatable reads and concurrent changes.
     * @param concurrentWriteClo Updating closure.
     * @param afterReadClo Closure making write changes that should also be made inside repeatable read transaction
     *     (must yield an exception).
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void doTestRepeatableRead(final IgniteInClosure<Connection> concurrentWriteClo,
        final IgniteInClosure<Connection> afterReadClo) throws Exception {
        final CountDownLatch repeatableReadLatch = new CountDownLatch(1);

        final CountDownLatch initLatch = new CountDownLatch(1);

        final IgniteInternalFuture<?> readFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                executeInTransaction(new TransactionClosure() {
                    @Override public void apply(Connection conn) {
                        List<?> before = flat(execute(conn, "SELECT * from \"Person\".Person where id = 1"));

                        assertEqualsCollections(l(1, "John", "Smith", 1, 1), before);

                        initLatch.countDown();

                        try {
                            U.await(repeatableReadLatch);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            throw new IgniteException(e);
                        }

                        List<?> after = flat(execute(conn, "SELECT * from \"Person\".Person where id = 1"));

                        assertEqualsCollections(before, after);

                        if (afterReadClo != null)
                            afterReadClo.apply(conn);
                    }
                });

                return null;
            }
        }, 1);

        IgniteInternalFuture<?> conModFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                executeInTransaction(new TransactionClosure() {
                    @Override public void apply(Connection conn) {
                        try {
                            U.await(initLatch);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            throw new IgniteException(e);
                        }

                        concurrentWriteClo.apply(conn);

                        repeatableReadLatch.countDown();
                    }
                });

                return null;
            }
        }, 1);

        conModFut.get();

        if (afterReadClo != null) {
            IgniteCheckedException ex = (IgniteCheckedException)GridTestUtils.assertThrows(null, new Callable() {
                @Override public Object call() throws Exception {
                    readFut.get();

                    return null;
                }
            }, IgniteCheckedException.class, "Mvcc version mismatch.");

            assertTrue(X.hasCause(ex, SQLException.class));

            assertTrue(X.getCause(ex).getMessage().contains("Mvcc version mismatch."));
        }
        else
            readFut.get();
    }

    /**
     * Create a new connection, a new transaction and run given closure in its scope.
     * @param clo Closure.
     * @throws SQLException if failed.
     */
    private void executeInTransaction(TransactionClosure clo) throws SQLException {
        try (Connection conn = connect()) {
            executeInTransaction(conn, clo);
        }
    }

    /**
     * Create a new transaction and run given closure in its scope.
     * @param conn Connection.
     * @param clo Closure.
     * @throws SQLException if failed.
     */
    private void executeInTransaction(Connection conn, TransactionClosure clo) throws SQLException {
        begin(conn);

        clo.apply(conn);

        commit(conn);
    }

    /**
     * @return Auto commit strategy for this test.
     */
    abstract boolean autoCommit();

    /**
     * @param c Connection to begin a transaction on.
     */
    private void begin(Connection c) throws SQLException {
        if (autoCommit())
            execute(c, "BEGIN");
    }

    /**
     * @param c Connection to begin a transaction on.
     */
    private void commit(Connection c) throws SQLException {
        if (autoCommit())
            execute(c, "COMMIT");
        else
            c.commit();
    }

    /**
     * @param c Connection to rollback a transaction on.
     */
    private void rollback(Connection c) {
        try {
            if (autoCommit())
                execute(c, "ROLLBACK");
            else
                c.rollback();
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param sql Statement.
     * @param args Arguments.
     * @return Result set.
     * @throws SQLException if failed.
     */
    List<List<?>> execute(String sql, Object... args) throws SQLException {
        try (Connection c = connect()) {
            c.setAutoCommit(true);

            return execute(c, sql, args);
        }
    }

    /**
     * @param sql Statement.
     * @param args Arguments.
     * @return Result set.
     * @throws RuntimeException if failed.
     */
    protected List<List<?>> execute(Connection conn, String sql, Object... args) {
        try {
            return super.execute(conn, sql, args);
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @return New connection to default node.
     * @throws SQLException if failed.
     */
    private Connection connect() throws SQLException {
        return connect(null);
    }

    /**
     * @param params Connection parameters.
     * @return New connection to default node.
     * @throws SQLException if failed.
     */
    private Connection connect(String params) throws SQLException {
        Connection c = connect(node(), params);

        c.setAutoCommit(false);

        return c;
    }

    /**
     * @param node Node to connect to.
     * @param params Connection parameters.
     * @return Thin JDBC connection to specified node.
     */
    protected Connection connect(IgniteEx node, String params) {
        try {
            return super.connect(node, params);
        }
        catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * @return Default node to fire queries from.
     */
    private IgniteEx node() {
        return grid(nodeIndex());
    }

    /**
     * @return {@link Person} cache.
     */
    private IgniteCache<Integer, Person> personCache() {
        return node().cache("Person");
    }

    /**
     * @return Node index to fire queries from.
     */
    abstract int nodeIndex();

    /**
     * @param id New person's id.
     * @param firstName First name.
     * @param lastName Second name.
     * @param cityId City id.
     * @param companyId Company id.
     * @throws SQLException if failed.
     */
    private void insertPerson(final int id, final String firstName, final String lastName, final int cityId,
        final int companyId) throws SQLException {
        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection conn) {
                insertPerson(conn, id, firstName, lastName, cityId, companyId);
            }
        });
    }

    /**
     * @param c Connection.
     * @param id New person's id.
     * @param firstName First name.
     * @param lastName Second name.
     * @param cityId City id.
     * @param companyId Company id.
     */
    private void insertPerson(Connection c, int id, String firstName, String lastName, int cityId, int companyId) {
        execute(c, "INSERT INTO \"Person\".person (id, firstName, lastName, cityId, companyId) values (?, ?, ?, ?, ?)",
            id, firstName, lastName, cityId, companyId);
    }

    /**
     * @param id New city's id.
     * @param name City name.
     * @param population Number of people.
     * @throws SQLException if failed.
     */
    private void insertCity(final int id, final String name, final int population) throws SQLException {
        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection conn) {
                insertCity(conn, id, name, population);
            }
        });
    }

    /**
     * @param c Connection.
     * @param id New city's id.
     * @param name City name.
     * @param population Number of people.
     */
    private void insertCity(Connection c, int id, String name, int population) {
        execute(c, "INSERT INTO city (id, name, population) values (?, ?, ?)", id, name, population);
    }

    /**
     * @param id New company's id.
     * @param name Company name.
     * @param cityId City id.
     * @throws SQLException if failed.
     */
    private void insertCompany(final int id, final String name, final int cityId) throws SQLException {
        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection conn) {
                insertCompany(conn, id, name, cityId);
            }
        });
    }

    /**
     * @param c Connection.
     * @param id New company's id.
     * @param name Company name.
     * @param cityId City id.
     */
    private void insertCompany(Connection c, int id, String name, int cityId) {
        execute(c, "INSERT INTO company (id, name, \"cityid\") values (?, ?, ?)", id, name, cityId);
    }

    /**
     * @param id New product's id.
     * @param name Product name.
     * @param companyId Company id..
     * @throws SQLException if failed.
     */
    private void insertProduct(final int id, final String name, final int companyId) throws SQLException {
        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection conn) {
                insertProduct(conn, id, name, companyId);
            }
        });
    }

    /**
     * @param c Connection.
     * @param id New product's id.
     * @param name Product name.
     * @param companyId Company id..
     */
    private void insertProduct(Connection c, int id, String name, int companyId) {
        execute(c, "INSERT INTO product (id, name, companyid) values (?, ?, ?)", id, name, companyId);
    }

    /**
     * Person class.
     */
    private final static class Person {
        /** */
        @QuerySqlField
        public int id;

        /** */
        @QuerySqlField
        public String firstName;

        /** */
        @QuerySqlField
        public String lastName;
    }

    /**
     * Closure to be executed in scope of a transaction.
     */
    private abstract class TransactionClosure implements IgniteInClosure<Connection> {
        // No-op.
    }

    /**
     * @return List of given arguments.
     */
    private static List<?> l(Object... args) {
        return F.asList(args);
    }

    /**
     * Flatten rows.
     * @param rows Rows.
     * @return Rows as a single list.
     */
    @SuppressWarnings("unchecked")
    private static <T> List<T> flat(Collection<? extends Collection<?>> rows) {
        return new ArrayList<>(F.flatCollections((Collection<? extends Collection<T>>)rows));
    }
}
