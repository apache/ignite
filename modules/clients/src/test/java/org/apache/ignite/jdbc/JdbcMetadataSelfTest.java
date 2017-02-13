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

package org.apache.ignite.jdbc;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.VARCHAR;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Metadata tests.
 */
public class JdbcMetadataSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL. */
    private static final String URL = "jdbc:ignite://127.0.0.1/";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setIndexedTypes(String.class, Organization.class, AffinityKey.class, Person.class);

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

        IgniteCache<String, Organization> orgCache = grid(0).cache(null);

        assert orgCache != null;

        orgCache.put("o1", new Organization(1, "A"));
        orgCache.put("o2", new Organization(2, "B"));

        IgniteCache<AffinityKey<String>, Person> personCache = grid(0).cache(null);

        assert personCache != null;

        personCache.put(new AffinityKey<>("p1", "o1"), new Person("John White", 25, 1));
        personCache.put(new AffinityKey<>("p2", "o1"), new Person("Joe Black", 35, 1));
        personCache.put(new AffinityKey<>("p3", "o2"), new Person("Mike Green", 40, 2));

        Class.forName("org.apache.ignite.IgniteJdbcDriver");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testResultSetMetaData() throws Exception {
        Statement stmt = DriverManager.getConnection(URL).createStatement();

        ResultSet rs = stmt.executeQuery(
            "select p.name, o.id as orgId from Person p, Organization o where p.orgId = o.id");

        assert rs != null;

        ResultSetMetaData meta = rs.getMetaData();

        assert meta != null;

        assert meta.getColumnCount() == 2;

        assert "Person".equalsIgnoreCase(meta.getTableName(1));
        assert "name".equalsIgnoreCase(meta.getColumnName(1));
        assert "name".equalsIgnoreCase(meta.getColumnLabel(1));
        assert meta.getColumnType(1) == VARCHAR;
        assert "VARCHAR".equals(meta.getColumnTypeName(1));
        assert "java.lang.String".equals(meta.getColumnClassName(1));

        assert "Organization".equalsIgnoreCase(meta.getTableName(2));
        assert "orgId".equalsIgnoreCase(meta.getColumnName(2));
        assert "orgId".equalsIgnoreCase(meta.getColumnLabel(2));
        assert meta.getColumnType(2) == INTEGER;
        assert "INTEGER".equals(meta.getColumnTypeName(2));
        assert "java.lang.Integer".equals(meta.getColumnClassName(2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTables() throws Exception {

        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            Collection<String> names = new ArrayList<>(2);

            names.add("PERSON");
            names.add("ORGANIZATION");

            ResultSet rs = meta.getTables("", "PUBLIC", "%", new String[]{"TABLE"});

            assert rs != null;

            int cnt = 0;

            while (rs.next()) {
                assert "TABLE".equals(rs.getString("TABLE_TYPE"));
                assert names.remove(rs.getString("TABLE_NAME"));

                cnt++;
            }

            assert names.isEmpty();
            assert cnt == 2;

            names.add("PERSON");
            names.add("ORGANIZATION");

            rs = meta.getTables("", "PUBLIC", "%", null);

            assert rs != null;

            cnt = 0;

            while (rs.next()) {
                assert "TABLE".equals(rs.getString("TABLE_TYPE"));
                assert names.remove(rs.getString("TABLE_NAME"));

                cnt++;
            }

            assert names.isEmpty();
            assert cnt == 2;

            rs = meta.getTables("", "PUBLIC", "", new String[]{"WRONG"});

            assert !rs.next();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetColumns() throws Exception {

        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns("", "PUBLIC", "Person", "%");

            assert rs != null;

            Collection<String> names = new ArrayList<>(2);

            names.add("NAME");
            names.add("AGE");
            names.add("ORGID");
            names.add("_KEY");
            names.add("_VAL");

            int cnt = 0;

            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");

                assert names.remove(name);

                if ("NAME".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == VARCHAR;
                    assert "VARCHAR".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 1;
                } else if ("AGE".equals(name) || "ORGID".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == INTEGER;
                    assert "INTEGER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                }
                if ("_KEY".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == OTHER;
                    assert "OTHER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                }
                if ("_VAL".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == OTHER;
                    assert "OTHER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                }

                cnt++;
            }

            assert names.isEmpty();
            assert cnt == 5;

            rs = meta.getColumns("", "PUBLIC", "Organization", "%");

            assert rs != null;

            names.add("ID");
            names.add("NAME");
            names.add("_KEY");
            names.add("_VAL");

            cnt = 0;

            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");

                assert names.remove(name);

                if ("id".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == INTEGER;
                    assert "INTEGER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                } else if ("name".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == VARCHAR;
                    assert "VARCHAR".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 1;
                }
                if ("_KEY".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == VARCHAR;
                    assert "VARCHAR".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                }
                if ("_VAL".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == OTHER;
                    assert "OTHER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                }

                cnt++;
            }

            assert names.isEmpty();
            assert cnt == 4;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetadataResultSetClose() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL);
             ResultSet tbls = conn.getMetaData().getTables(null, null, "%", null)) {
            int colCnt = tbls.getMetaData().getColumnCount();

            while (tbls.next()) {
                for (int i = 0; i < colCnt; i++)
                    tbls.getObject(i + 1);
            }
        }
        catch (Exception ignored) {
            fail();
        }
    }

    /**
     * Person.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Person implements Serializable {
        /** Name. */
        @QuerySqlField(index = false)
        private final String name;

        /** Age. */
        @QuerySqlField
        private final int age;

        /** Organization ID. */
        @QuerySqlField
        private final int orgId;

        /**
         * @param name Name.
         * @param age Age.
         * @param orgId Organization ID.
         */
        private Person(String name, int age, int orgId) {
            assert !F.isEmpty(name);
            assert age > 0;
            assert orgId > 0;

            this.name = name;
            this.age = age;
            this.orgId = orgId;
        }
    }

    /**
     * Organization.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Organization implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** Name. */
        @QuerySqlField(index = false)
        private final String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        private Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
