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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
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

        LinkedHashMap<String, Boolean> persFields = new LinkedHashMap<>();

        persFields.put("name", true);
        persFields.put("age", false);

        cfg.setCacheConfiguration(
            cacheConfiguration("pers").setQueryEntities(Arrays.asList(
                new QueryEntityEx(
                    new QueryEntity(AffinityKey.class.getName(), Person.class.getName())
                        .addQueryField("name", String.class.getName(), null)
                        .addQueryField("age", Integer.class.getName(), null)
                        .addQueryField("orgId", Integer.class.getName(), null)
                        .setIndexes(Arrays.asList(
                            new QueryIndex("orgId"),
                            new QueryIndex().setFields(persFields))))
                    .setNotNullFields(new HashSet<>(Arrays.asList("age", "name")))
            )),
            cacheConfiguration("org").setQueryEntities(Arrays.asList(
                new QueryEntity(AffinityKey.class, Organization.class))));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        return cfg;
    }

    /**
     * @param name Name.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(@NotNull String name) {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setName(name);
        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setAtomicityMode(TRANSACTIONAL);

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
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns("", "pers", "PERSON", "%");

            assertNotNull(rs);

            assertEquals(24, rs.getMetaData().getColumnCount());

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
                    assertEquals(0, rs.getInt("NULLABLE"));
                    assertEquals(0, rs.getInt(11)); // nullable column by index
                    assertEquals("NO", rs.getString("IS_NULLABLE"));
                } else if ("AGE".equals(name)) {
                    assertEquals(INTEGER, rs.getInt("DATA_TYPE"));
                    assertEquals("INTEGER", rs.getString("TYPE_NAME"));
                    assertEquals(0, rs.getInt("NULLABLE"));
                    assertEquals(0, rs.getInt(11)); // nullable column by index
                    assertEquals("NO", rs.getString("IS_NULLABLE"));
                } else if ("ORGID".equals(name)) {
                    assertEquals(INTEGER, rs.getInt("DATA_TYPE"));
                    assertEquals("INTEGER", rs.getString("TYPE_NAME"));
                    assertEquals(1, rs.getInt("NULLABLE"));
                    assertEquals(1, rs.getInt(11)); // nullable column by index
                    assertEquals("YES", rs.getString("IS_NULLABLE"));
                }

                cnt++;
            }

            assertTrue(names.isEmpty());
            assertEquals(3, cnt);

            rs = meta.getColumns("", "org", "ORGANIZATION", "%");

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
                    assertEquals(0, rs.getInt(11)); // nullable column by index
                    assertEquals("NO", rs.getString("IS_NULLABLE"));
                } else if ("name".equals(name)) {
                    assertEquals(VARCHAR, rs.getInt("DATA_TYPE"));
                    assertEquals("VARCHAR", rs.getString("TYPE_NAME"));
                    assertEquals(1, rs.getInt("NULLABLE"));
                    assertEquals(1, rs.getInt(11)); // nullable column by index
                    assertEquals("YES", rs.getString("IS_NULLABLE"));
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
     * @throws Exception If failed.
     */
    public void testIndexMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL);
             ResultSet rs = conn.getMetaData().getIndexInfo(null, "pers", "PERSON", false, false)) {

            int cnt = 0;

            while (rs.next()) {
                String idxName = rs.getString("INDEX_NAME");
                String field = rs.getString("COLUMN_NAME");
                String ascOrDesc = rs.getString("ASC_OR_DESC");

                assertEquals(DatabaseMetaData.tableIndexOther, rs.getInt("TYPE"));

                if ("PERSON_ORGID_ASC_IDX".equals(idxName)) {
                    assertEquals("ORGID", field);
                    assertEquals("A", ascOrDesc);
                }
                else if ("PERSON_NAME_ASC_AGE_DESC_IDX".equals(idxName)) {
                    if ("NAME".equals(field))
                        assertEquals("A", ascOrDesc);
                    else if ("AGE".equals(field))
                        assertEquals("D", ascOrDesc);
                    else
                        fail("Unexpected field: " + field);
                }
                else
                    fail("Unexpected index: " + idxName);

                cnt++;
            }

            assertEquals(3, cnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryKeyMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL);
             ResultSet rs = conn.getMetaData().getPrimaryKeys(null, "pers", "PERSON")) {

            int cnt = 0;

            while (rs.next()) {
                assertEquals("_KEY", rs.getString("COLUMN_NAME"));

                cnt++;
            }

            assertEquals(1, cnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testParametersMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            conn.setSchema("pers");

            PreparedStatement stmt = conn.prepareStatement("select orgId from Person p where p.name > ? and p.orgId > ?");

            ParameterMetaData meta = stmt.getParameterMetaData();

            assertNotNull(meta);

            assertEquals(2, meta.getParameterCount());

            assertEquals(Types.VARCHAR, meta.getParameterType(1));
            assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
            assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

            assertEquals(Types.INTEGER, meta.getParameterType(2));
            assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSchemasMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            ResultSet rs = conn.getMetaData().getSchemas();

            Set<String> expectedSchemas = new HashSet<>(Arrays.asList("pers", "org"));

            Set<String> schemas = new HashSet<>();

            while (rs.next()) {
                schemas.add(rs.getString(1));

                assertNull(rs.getString(2));
            }

            assertEquals(expectedSchemas, schemas);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testVersions() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            assertEquals("Apache Ignite", conn.getMetaData().getDatabaseProductName());
            assertEquals(JdbcDatabaseMetadata.DRIVER_NAME, conn.getMetaData().getDriverName());
            assertEquals(IgniteVersionUtils.VER.toString(), conn.getMetaData().getDatabaseProductVersion());
            assertEquals(IgniteVersionUtils.VER.toString(), conn.getMetaData().getDriverVersion());
            assertEquals(IgniteVersionUtils.VER.major(), conn.getMetaData().getDatabaseMajorVersion());
            assertEquals(IgniteVersionUtils.VER.major(), conn.getMetaData().getDriverMajorVersion());
            assertEquals(IgniteVersionUtils.VER.minor(), conn.getMetaData().getDatabaseMinorVersion());
            assertEquals(IgniteVersionUtils.VER.minor(), conn.getMetaData().getDriverMinorVersion());
            assertEquals(4, conn.getMetaData().getJDBCMajorVersion());
            assertEquals(1, conn.getMetaData().getJDBCMinorVersion());
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
