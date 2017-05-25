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
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Metadata tests.
 */
public class JdbcMetadataSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** JDBC URL. */
    private static final String BASE_URL = CFG_URL_PREFIX + "cache=pers@modules/clients/src/test/config/jdbc-config.xml";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            cacheConfiguration("pers", AffinityKey.class, Person.class),
            cacheConfiguration("org", String.class, Organization.class));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /**
     * @param name Name.
     * @param clsK Class k.
     * @param clsV Class v.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(@NotNull String name, Class<?> clsK, Class<?> clsV) {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setName(name);
        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setAtomicityMode(TRANSACTIONAL);
        cache.setIndexedTypes(clsK, clsV);

        return cache;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);

        IgniteCache<String, Organization> orgCache = grid(0).cache("org");

        orgCache.put("o1", new Organization(1, "A"));
        orgCache.put("o2", new Organization(2, "B"));

        IgniteCache<AffinityKey<String>, Person> personCache = grid(0).cache("pers");

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
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(
                "select p.name, o.id as orgId from \"pers\".Person p, \"org\".Organization o where p.orgId = o.id");

            assertNotNull(rs);

            ResultSetMetaData meta = rs.getMetaData();

            assertNotNull(meta);

            assertEquals(2, meta.getColumnCount());

            assertTrue("Person".equalsIgnoreCase(meta.getTableName(1)));
            assertTrue("name".equalsIgnoreCase(meta.getColumnName(1)));
            assertTrue("name".equalsIgnoreCase(meta.getColumnLabel(1)));
            assertEquals(VARCHAR, meta.getColumnType(1));
            assertEquals("VARCHAR", meta.getColumnTypeName(1));
            assertEquals("java.lang.String", meta.getColumnClassName(1));

            assertTrue("Organization".equalsIgnoreCase(meta.getTableName(2)));
            assertTrue("orgId".equalsIgnoreCase(meta.getColumnName(2)));
            assertTrue("orgId".equalsIgnoreCase(meta.getColumnLabel(2)));
            assertEquals(INTEGER, meta.getColumnType(2));
            assertEquals("INTEGER", meta.getColumnTypeName(2));
            assertEquals("java.lang.Integer", meta.getColumnClassName(2));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetTables() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getTables("", "pers", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            rs = meta.getTables("", "org", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            rs = meta.getTables("", "pers", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            rs = meta.getTables("", "org", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            rs = meta.getTables("", "PUBLIC", "", new String[]{"WRONG"});
            assertFalse(rs.next());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetColumns() throws Exception {
        final boolean primitivesInformationIsLostAfterStore = ignite(0).configuration().getMarshaller() instanceof BinaryMarshaller;
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns("", "pers", "Person", "%");

            assertNotNull(rs);

            Collection<String> names = new ArrayList<>(2);

            names.add("NAME");
            names.add("AGE");
            names.add("ORGID");

            int cnt = 0;

            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");

                assertTrue(names.remove(name));

                if ("NAME".equals(name)) {
                    assertEquals(VARCHAR, rs.getInt("DATA_TYPE"));
                    assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
                    assertEquals(1, rs.getInt("NULLABLE"));
                } else if ("AGE".equals(name) || "ORGID".equals(name)) {
                    assertEquals(INTEGER, rs.getInt("DATA_TYPE"));
                    assertEquals("INTEGER", rs.getString("TYPE_NAME"));
                    assertEquals(primitivesInformationIsLostAfterStore ? 1 : 0, rs.getInt("NULLABLE"));
                }

                cnt++;
            }

            assertTrue(names.isEmpty());
            assertEquals(3, cnt);

            rs = meta.getColumns("", "org", "Organization", "%");

            assertNotNull(rs);

            names.add("ID");
            names.add("NAME");

            cnt = 0;

            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");

                assertTrue(names.remove(name));

                if ("id".equals(name)) {
                    assertEquals(INTEGER, rs.getInt("DATA_TYPE"));
                    assertEquals("INTEGER", rs.getString("TYPE_NAME"));
                    assertEquals(0, rs.getInt("NULLABLE"));
                } else if ("name".equals(name)) {
                    assertEquals(VARCHAR, rs.getInt("DATA_TYPE"));
                    assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
                    assertEquals(1, rs.getInt("NULLABLE"));
                }

                cnt++;
            }

            assertTrue(names.isEmpty());
            assertEquals(2, cnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetadataResultSetClose() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL);
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
