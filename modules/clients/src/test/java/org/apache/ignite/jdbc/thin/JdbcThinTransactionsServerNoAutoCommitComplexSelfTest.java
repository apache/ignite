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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Test to check various transactional scenarios.
 */
public class JdbcThinTransactionsServerNoAutoCommitComplexSelfTest extends JdbcThinAbstractSelfTest {
    /** Client node index. */
    final static int CLI_IDX = 1;

    private final static String PERSON_INSERT =
        "INSERT INTO \"Person\".person (id, firstName, lastName, cityId, companyId) values (?, ?, ?, ?, ?)";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String testIgniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(testIgniteInstanceName);

        cfg.setMvccEnabled(true);

        CacheConfiguration<Integer, Person> ccfg = new CacheConfiguration<>("Person");

        ccfg.setIndexedTypes(Integer.class, Person.class);

        ccfg.getQueryEntities().iterator().next().setKeyFieldName("id");

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        // Let the node with index 1 be client node.
        cfg.setClientMode(F.eq(testIgniteInstanceName, getTestIgniteInstanceName(CLI_IDX)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        execute("ALTER TABLE \"Person\".person add if not exists companyid int");

        execute("ALTER TABLE \"Person\".person add if not exists cityid int");

        execute("CREATE TABLE City (id int primary key, name varchar, state varchar, population int) WITH " +
            "\"atomicity=transactional,template=partitioned,backups=1\"");

        execute("CREATE TABLE Company (id int primary key, name varchar, cityid int) WITH " +
            "\"atomicity=transactional,template=replicated\"");

        insertPerson(1, "John", "Smith", 1, 1);

        insertPerson(2, "Mike", "Johns", 1, 2);

        insertPerson(3, "Sam", "Jules", 2, 2);

        insertPerson(4, "Alex", "Pope", 2, 3);

        insertPerson(5, "Peter", "Williams", 2, 3);
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

        assertEquals(Collections.singletonList(Arrays.asList(6, "John", "Doe", 2, 2)),
            execute("SELECT * FROM \"Person\".Person where id = 6"));
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
    boolean autoCommit() {
        return false;
    }

    /**
     * @param c Connection to begin a transaction on.
     */
    void begin(Connection c) throws SQLException {
        if (autoCommit())
            execute(c, "BEGIN");
    }

    /**
     * @param c Connection to begin a transaction on.
     */
    void commit(Connection c) throws SQLException {
        if (autoCommit())
            execute(c, "COMMIT");
        else
            c.commit();
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
     * @throws SQLException if failed.
     */
    List<List<?>> execute(Connection conn, String sql, Object... args) {
        try {
            List<List<?>> res = new ArrayList<>();

            try (PreparedStatement s = conn.prepareStatement(sql)) {
                for (int i = 0; i < args.length; i++)
                    s.setObject(i + 1, args[i]);

                if (s.execute()) {
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
                }
            }

            return res;
        }
        catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * @return New connection to default node.
     * @throws SQLException if failed.
     */
    Connection connect() throws SQLException {
        Connection c = AbstractSchemaSelfTest.connect(node());

        c.setAutoCommit(false);

        return c;
    }

    /**
     * @return Default node to fire queries from.
     */
    IgniteEx node() {
        return grid(nodeIndex());
    }

    /**
     * @return {@link Person} cache.
     */
    IgniteCache<Integer, Person> personCache() {
        return node().cache("Person");
    }

    /**
     * @return Node index to fire queries from.
     */
    int nodeIndex() {
        return 0;
    }

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

    private void insertCity(int id, String name, int population) throws SQLException {
        execute("INSERT INTO \"City\".city (id, name, population) values (?, ?, ?)",
            id, name, population);
    }

    private void insertCompany(int id, String name, int cityId) throws SQLException {
        execute("INSERT INTO \"Company\".company (id, name, cityid) values (?, ?, ?)",
            id, name, cityId);
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
     *
     */
    private abstract class TransactionClosure implements IgniteInClosure<Connection> {
        // No-op.
    }
}
