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

package org.apache.ignite.internal.jdbc2;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Statement test.
 */
public class JdbcInsertStatementSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX + "modules/clients/src/test/config/jdbc-config.xml";

    /** SQL query. */
    private static final String SQL = "insert into Person(_key, id, firstName, lastName, age) values " +
        "('p1', 1, 'John', 'White', 25), " +
        "('p2', 2, 'Joe', 'Black', 35), " +
        "('p3', 3, 'Mike', 'Green', 40)";

    /** SQL query. */
    private static final String SQL_SELECT = "select * from Person";

    /** SQL query. */
    private static final String SQL_PREPARED = "insert into Person(_key, id, firstName, lastName, age) values " +
        "(?, ?, ?, ?, ?), (?, ?, ?, ?, ?)";

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** Prepared statement. */
    private PreparedStatement prepStmt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(
            String.class, Person.class
        );

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);

        Class.forName("org.apache.ignite.IgniteJdbcDriver");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        conn = DriverManager.getConnection(BASE_URL);
        stmt = conn.createStatement();
        prepStmt = conn.prepareStatement(SQL_PREPARED);

        assertNotNull(stmt);
        assertFalse(stmt.isClosed());

        assertNotNull(prepStmt);
        assertFalse(prepStmt.isClosed());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try (Statement selStmt = conn.createStatement()) {
            assert selStmt.execute(SQL_SELECT);

            ResultSet rs = selStmt.getResultSet();

            assert rs != null;

            while (rs.next()) {
                int id = rs.getInt("id");

                switch (id) {
                    case 1:
                        assert "p1".equals(rs.getString("_key"));
                        assert "John".equals(rs.getString("firstName"));
                        assert "White".equals(rs.getString("lastName"));
                        assert rs.getInt("age") == 25;
                        break;

                    case 2:
                        assert "p2".equals(rs.getString("_key"));
                        assert "Joe".equals(rs.getString("firstName"));
                        assert "Black".equals(rs.getString("lastName"));
                        assert rs.getInt("age") == 35;
                        break;

                    case 3:
                        assert "p3".equals(rs.getString("_key"));
                        assert "Mike".equals(rs.getString("firstName"));
                        assert "Green".equals(rs.getString("lastName"));
                        assert rs.getInt("age") == 40;
                        break;

                    case 4:
                        assert "p4".equals(rs.getString("_key"));
                        assert "Leah".equals(rs.getString("firstName"));
                        assert "Grey".equals(rs.getString("lastName"));
                        assert rs.getInt("age") == 22;
                        break;

                    default:
                        assert false : "Invalid ID: " + id;
                }
            }
        }

        grid(0).cache(null).clear();

        assertEquals(0, grid(0).cache(null).size(CachePeekMode.ALL));

        if (stmt != null && !stmt.isClosed())
            stmt.close();

        if (prepStmt != null && !prepStmt.isClosed())
            prepStmt.close();

        conn.close();

        assertTrue(prepStmt.isClosed());
        assertTrue(stmt.isClosed());
        assertTrue(conn.isClosed());
    }

    /**
     * @throws SQLException If failed.
     */
    public void testExecuteUpdate() throws SQLException {
        int res = stmt.executeUpdate(SQL);

        assertEquals(3, res);
    }

    /**
     * @throws SQLException If failed.
     */
    public void testExecute() throws SQLException {
        boolean res = stmt.execute(SQL);

        assertEquals(false, res);
    }

    /**
     * @throws SQLException if failed.
     */
    public void testBatch() throws SQLException {
        prepStmt.setString(1, "p1");
        prepStmt.setInt(2, 1);
        prepStmt.setString(3, "John");
        prepStmt.setString(4, "White");
        prepStmt.setInt(5, 25);

        prepStmt.setString(6, "p2");
        prepStmt.setInt(7, 2);
        prepStmt.setString(8, "Joe");
        prepStmt.setString(9, "Black");
        prepStmt.setInt(10, 35);
        prepStmt.addBatch();

        prepStmt.setString(1, "p3");
        prepStmt.setInt(2, 3);
        prepStmt.setString(3, "Mike");
        prepStmt.setString(4, "Green");
        prepStmt.setInt(5, 40);

        prepStmt.setString(6, "p4");
        prepStmt.setInt(7, 4);
        prepStmt.setString(8, "Leah");
        prepStmt.setString(9, "Grey");
        prepStmt.setInt(10, 22);
        prepStmt.addBatch();

        int[] res = prepStmt.executeBatch();

        assertEquals(2, res[0]);
        assertEquals(2, res[1]);
    }

    /**
     *
     */
    public void testDuplicateKeys() {
        jcache(0).put("p2", new Person(2, "Joe", "Black", 35));

        Throwable reason = GridTestUtils.assertThrows(log, new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                return stmt.execute(SQL);
            }
        }, SQLException.class, null);

        assertNotNull(reason.getCause());

        reason = reason.getCause().getCause();

        assertNotNull(reason);

        assertEquals(IgniteException.class, reason.getClass());

        assertEquals("Failed to INSERT some keys because they are already in cache [keys=[p2]]", reason.getMessage());

        assertEquals(3, jcache(0).getAll(new HashSet<>(Arrays.asList("p1", "p2", "p3"))).size());
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
