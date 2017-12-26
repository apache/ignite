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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Test to check various transactional scenarios.
 */
public abstract class JdbcThinTransactionsAbstractComplexSelfTest extends JdbcThinAbstractSelfTest {
    /** Client node index. */
    final static int CLI_IDX = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String testIgniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(testIgniteInstanceName);

        cfg.setMvccEnabled(true);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>("Person");

        ccfg.setIndexedTypes(Integer.class, Person.class);

        ccfg.getQueryEntities().iterator().next().setKeyFieldName("id");

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        ccfg.setCacheMode(CacheMode.REPLICATED);

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
            "\"atomicity=transactional,template=partitioned,cache_name=City\"");

        execute("CREATE TABLE Company (id int, \"cityid\" int, name varchar, primary key (id, \"cityid\")) WITH " +
            "\"atomicity=transactional,template=partitioned,backups=1,wrap_value=false,affinity_key=cityid," +
            "cache_name=Company\"");

        execute("CREATE INDEX IF NOT EXISTS pidx ON \"Person\".person(cityid)");

        insertPerson(1, "John", "Smith", 1, 1);

        insertPerson(2, "Mike", "Johns", 1, 2);

        insertPerson(3, "Sam", "Jules", 2, 2);

        insertPerson(4, "Alex", "Pope", 2, 3);

        insertPerson(5, "Peter", "Williams", 2, 3);

        insertCity(1, "Los Angeles", 5000);

        insertCity(2, "Seattle", 1500);

        insertCity(3, "New York", 12000);

        insertCity(4, "Palo Alto", 400);

        insertCompany(1, "Microsoft", 2);

        insertCompany(2, "Google", 3);

        insertCompany(3, "Facebook", 1);

        insertCompany(4, "Uber", 1);

        insertCompany(5, "Apple", 4);
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
        personCache().clear();

        execute("DROP TABLE City");

        execute("DROP TABLE Company");

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

                insertPerson(conn, 7, "Mary", "Lee", 1, 3);
            }
        });

        assertEquals(l(
            l(6, "John", "Doe", 2, 2),
            l(7, "Mary", "Lee", 1, 3)
        ), execute("SELECT * FROM \"Person\".Person where id > 5 order by id"));
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


        assertEquals(l(l(5, "St Petersburg", 6000, 6, 5, "VK", 6, "Peter", "Sergeev", 5, 6)),
            execute("SELECT * FROM City left join Company on City.id = Company.\"cityid\" " +
                "left join \"Person\".Person p on City.id = p.cityid WHERE p.id = 6"));
    }

    /**
     *
     */
    public void testInsertFromExpression() throws SQLException {
        fail("https://issues.apache.org/jira/browse/IGNITE-7300");

        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection connection) {
                execute(connection, "insert into city (id, name, population) values (? + 1, ?, ?)",
                    8, "Moscow", 15000);
            }
        });
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
    private void rollback(Connection c) throws SQLException {
        if (autoCommit())
            execute(c, "ROLLBACK");
        else
            c.rollback();
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
    List<List<?>> execute(Connection conn, String sql, Object... args) {
        try {
            try (PreparedStatement s = conn.prepareStatement(sql)) {
                for (int i = 0; i < args.length; i++)
                    s.setObject(i + 1, args[i]);

                if (s.execute()) {
                    List<List<?>> res = new ArrayList<>();

                    try (ResultSet rs = s.getResultSet()) {
                        ResultSetMetaData meta = rs.getMetaData();

                        int cnt = meta.getColumnCount();

                        while (rs.next()) {
                            List<Object> row = new ArrayList<>(cnt);

                            for (int i = 1; i <= cnt; i++)
                                row.add(rs.getObject(i));

                            res.add(row);
                        }
                    }

                    return res;
                }
                else
                    return Collections.emptyList();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
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
    private static Connection connect(IgniteEx node, String params) {
        Collection<GridPortRecord> recs = node.context().ports().records();

        GridPortRecord cliLsnrRec = null;

        for (GridPortRecord rec : recs) {
            if (rec.clazz() == ClientListenerProcessor.class) {
                cliLsnrRec = rec;

                break;
            }
        }

        assertNotNull(cliLsnrRec);

        try {
            String connStr = "jdbc:ignite:thin://127.0.0.1:" + cliLsnrRec.port();

            if (!F.isEmpty(params))
                connStr += "/?" + params;

            return DriverManager.getConnection(connStr);
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
     * @throws SQLException
     */
    private void insertPerson(final int id, final String firstName, final String lastName, final int cityId,
        final int companyId) throws SQLException {
        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection conn) {
                insertPerson(conn, id, firstName, lastName, cityId, companyId);
            }
        });
    }

    private void insertPerson(Connection c, int id, String firstName, String lastName, int cityId, int companyId) {
        execute(c, "INSERT INTO \"Person\".person (id, firstName, lastName, cityId, companyId) values (?, ?, ?, ?, ?)",
            id, firstName, lastName, cityId, companyId);
    }

    private void insertCity(final int id, final String name, final int population) throws SQLException {
        executeInTransaction(new TransactionClosure() {
            @Override public void apply(Connection conn) {
                insertCity(conn, id, name, population);
            }
        });
    }

    private void insertCity(Connection c, int id, String name, int population) {
        execute(c, "INSERT INTO city (id, name, population) values (?, ?, ?)", id, name, population);
    }

    private void insertCompany(final int id, final String name, final int cityId) throws SQLException {
        executeInTransaction(new TransactionClosure() {
            @Override
            public void apply(Connection conn) {
                insertCompany(conn, id, name, cityId);
            }
        });
    }

    private void insertCompany(Connection c, int id, String name, int cityId) {
        execute(c, "INSERT INTO company (id, name, \"cityid\") values (?, ?, ?)", id, name, cityId);
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
}
