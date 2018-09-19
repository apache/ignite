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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.VARCHAR;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Metadata tests.
 */
public class JdbcThinMetadataSelfTest extends JdbcThinAbstractSelfTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @param qryEntity Query entity.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(QueryEntity qryEntity) {
        CacheConfiguration<?,?> cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);

        cache.setQueryEntities(Collections.singletonList(qryEntity));

        return cache;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);

        Map<String, Integer> orgPrecision = new HashMap<>();
        
        orgPrecision.put("name", 42);

        IgniteCache<String, Organization> orgCache = jcache(grid(0),
            cacheConfiguration(new QueryEntity(String.class.getName(), Organization.class.getName())
                .addQueryField("id", Integer.class.getName(), null)
                .addQueryField("name", String.class.getName(), null)
                .setFieldsPrecision(orgPrecision)
                .setIndexes(Arrays.asList(
                    new QueryIndex("id"),
                    new QueryIndex("name", false, "org_name_index")
                ))), "org");

        assert orgCache != null;

        orgCache.put("o1", new Organization(1, "A"));
        orgCache.put("o2", new Organization(2, "B"));

        LinkedHashMap<String, Boolean> persFields = new LinkedHashMap<>();

        persFields.put("name", true);
        persFields.put("age", false);

        IgniteCache<AffinityKey, Person> personCache = jcache(grid(0), cacheConfiguration(
            new QueryEntityEx(
                new QueryEntity(AffinityKey.class.getName(), Person.class.getName())
                    .addQueryField("name", String.class.getName(), null)
                    .addQueryField("age", Integer.class.getName(), null)
                    .addQueryField("orgId", Integer.class.getName(), null)
                    .setIndexes(Arrays.asList(
                        new QueryIndex("orgId"),
                        new QueryIndex().setFields(persFields))))
                .setNotNullFields(new HashSet<>(Arrays.asList("age", "name")))
            ), "pers");

        assert personCache != null;

        personCache.put(new AffinityKey<>("p1", "o1"), new Person("John White", 25, 1));
        personCache.put(new AffinityKey<>("p2", "o1"), new Person("Joe Black", 35, 1));
        personCache.put(new AffinityKey<>("p3", "o2"), new Person("Mike Green", 40, 2));

        IgniteCache<Integer, Department> departmentCache = jcache(grid(0), 
            defaultCacheConfiguration().setIndexedTypes(Integer.class, Department.class), "dep");

        try (Connection conn = DriverManager.getConnection(URL)) {
            Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE TEST (ID INT, NAME VARCHAR(50) default 'default name', " +
                "age int default 21, VAL VARCHAR(50), PRIMARY KEY (ID, NAME))");
            stmt.execute("CREATE TABLE \"Quoted\" (\"Id\" INT primary key, \"Name\" VARCHAR(50)) WITH WRAP_KEY");
            stmt.execute("CREATE INDEX \"MyTestIndex quoted\" on \"Quoted\" (\"Id\" DESC)");
            stmt.execute("CREATE INDEX IDX ON TEST (ID ASC)");
            stmt.execute("CREATE TABLE TEST_DECIMAL_COLUMN (ID INT primary key, DEC_COL DECIMAL(8, 3))");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testResultSetMetaData() throws Exception {
        Connection conn = DriverManager.getConnection(URL);

        conn.setSchema("\"pers\"");

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery(
            "select p.name, o.id as orgId from \"pers\".Person p, \"org\".Organization o where p.orgId = o.id");

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

            ResultSet rs = meta.getTables("", "pers", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            assertFalse(rs.next());

            rs = meta.getTables("", "org", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            assertFalse(rs.next());

            rs = meta.getTables("", "pers", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            assertFalse(rs.next());

            rs = meta.getTables("", "org", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            assertFalse(rs.next());

            rs = meta.getTables("", "PUBLIC", "", new String[]{"WRONG"});
            assertFalse(rs.next());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllTables() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getTables(null, null, null, null);

            Set<String> expectedTbls = new HashSet<>(Arrays.asList(
                "org.ORGANIZATION",
                "pers.PERSON",
                "dep.DEPARTMENT",
                "PUBLIC.TEST",
                "PUBLIC.Quoted",
                "PUBLIC.TEST_DECIMAL_COLUMN"));

            Set<String> actualTbls = new HashSet<>(expectedTbls.size());

            while(rs.next()) {
                actualTbls.add(rs.getString("TABLE_SCHEM") + '.'
                    + rs.getString("TABLE_NAME"));
            }

            assert expectedTbls.equals(actualTbls) : "expectedTbls=" + expectedTbls +
                ", actualTbls" + actualTbls;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setSchema("pers");

            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns("", "pers", "PERSON", "%");

            ResultSetMetaData rsMeta = rs.getMetaData();

            assert rsMeta.getColumnCount() == 24 : "Invalid columns count: " + rsMeta.getColumnCount();

            assert rs != null;

            Collection<String> names = new ArrayList<>(2);

            names.add("NAME");
            names.add("AGE");
            names.add("ORGID");

            int cnt = 0;

            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");

                assert names.remove(name);

                if ("NAME".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == VARCHAR;
                    assert "VARCHAR".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                    assert rs.getInt(11) == 0; // nullable column by index
                    assert rs.getString("IS_NULLABLE").equals("NO");
                } else if ("ORGID".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == INTEGER;
                    assert "INTEGER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 1;
                    assert rs.getInt(11) == 1;  // nullable column by index
                    assert rs.getString("IS_NULLABLE").equals("YES");
                } else if ("AGE".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == INTEGER;
                    assert "INTEGER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                    assert rs.getInt(11) == 0;  // nullable column by index
                    assert rs.getString("IS_NULLABLE").equals("NO");
                }
                else if ("_KEY".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == OTHER;
                    assert "OTHER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                    assert rs.getInt(11) == 0;  // nullable column by index
                    assert rs.getString("IS_NULLABLE").equals("NO");
                }
                else if ("_VAL".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == OTHER;
                    assert "OTHER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 0;
                    assert rs.getInt(11) == 0;  // nullable column by index
                    assert rs.getString("IS_NULLABLE").equals("NO");
                }

                cnt++;
            }

            assert names.isEmpty();
            assert cnt == 3;

            rs = meta.getColumns("", "org", "ORGANIZATION", "%");

            assert rs != null;

            names.add("ID");
            names.add("NAME");

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
            assert cnt == 2;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns(null, null, null, null);

            Set<String> expectedCols = new HashSet<>(Arrays.asList(
                "org.ORGANIZATION.ID.null",
                "org.ORGANIZATION.NAME.null.42",
                "pers.PERSON.ORGID.null",
                "pers.PERSON.AGE.null",
                "pers.PERSON.NAME.null",
                "dep.DEPARTMENT.ID.null",
                "dep.DEPARTMENT.NAME.null.43",
                "PUBLIC.TEST.ID.null",
                "PUBLIC.TEST.NAME.'default name'.50",
                "PUBLIC.TEST.VAL.null.50",
                "PUBLIC.TEST.AGE.21",
                "PUBLIC.Quoted.Id.null",
                "PUBLIC.Quoted.Name.null.50",
                "PUBLIC.TEST_DECIMAL_COLUMN.ID.null",
                "PUBLIC.TEST_DECIMAL_COLUMN.DEC_COL.null.8.3"
            ));

            Set<String> actualCols = new HashSet<>(expectedCols.size());

            while(rs.next()) {
                int precision = rs.getInt("COLUMN_SIZE");

                int scale = rs.getInt("DECIMAL_DIGITS");

                actualCols.add(rs.getString("TABLE_SCHEM") + '.'
                    + rs.getString("TABLE_NAME") + "."
                    + rs.getString("COLUMN_NAME") + "."
                    + rs.getString("COLUMN_DEF")
                    + (precision == 0 ? "" : ("." + precision))
                    + (scale == 0 ? "" : ("." + scale))
                );
            }

            assert expectedCols.equals(actualCols) : "expectedCols=" + expectedCols +
                ", actualCols" + actualCols;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidCatalog() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getSchemas("q", null);

            assert !rs.next() : "Results must be empty";

            rs = meta.getTables("q", null, null, null);

            assert !rs.next() : "Results must be empty";

            rs = meta.getColumns("q", null, null, null);

            assert !rs.next() : "Results must be empty";

            rs = meta.getIndexInfo("q", null, null, false, false);

            assert !rs.next() : "Results must be empty";

            rs = meta.getPrimaryKeys("q", null, null);

            assert !rs.next() : "Results must be empty";
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIndexMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL);
             ResultSet rs = conn.getMetaData().getIndexInfo(null, "pers", "PERSON", false, false)) {

            int cnt = 0;

            while (rs.next()) {
                String idxName = rs.getString("INDEX_NAME");
                String field = rs.getString("COLUMN_NAME");
                String ascOrDesc = rs.getString("ASC_OR_DESC");

                assert rs.getShort("TYPE") == DatabaseMetaData.tableIndexOther;

                if ("PERSON_ORGID_ASC_IDX".equals(idxName)) {
                    assert "ORGID".equals(field);
                    assert "A".equals(ascOrDesc);
                }
                else if ("PERSON_NAME_ASC_AGE_DESC_IDX".equals(idxName)) {
                    if ("NAME".equals(field))
                        assert "A".equals(ascOrDesc);
                    else if ("AGE".equals(field))
                        assert "D".equals(ascOrDesc);
                    else
                        fail("Unexpected field: " + field);
                }
                else
                    fail("Unexpected index: " + idxName);

                cnt++;
            }

            assert cnt == 3;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllIndexes() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getIndexInfo(null, null, null, false, false);

            Set<String> expectedIdxs = new HashSet<>(Arrays.asList(
                "org.ORGANIZATION.ORGANIZATION_ID_ASC_IDX",
                "org.ORGANIZATION.ORG_NAME_INDEX",
                "pers.PERSON.PERSON_ORGID_ASC_IDX",
                "pers.PERSON.PERSON_NAME_ASC_AGE_DESC_IDX",
                "PUBLIC.TEST.IDX",
                "PUBLIC.Quoted.MyTestIndex quoted"));

            Set<String> actualIdxs = new HashSet<>(expectedIdxs.size());

            while(rs.next()) {
                actualIdxs.add(rs.getString("TABLE_SCHEM") +
                    '.' + rs.getString("TABLE_NAME") +
                    '.' + rs.getString("INDEX_NAME"));
            }

            assert expectedIdxs.equals(actualIdxs) : "expectedIdxs=" + expectedIdxs +
                ", actualIdxs" + actualIdxs;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryKeyMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL);
             ResultSet rs = conn.getMetaData().getPrimaryKeys(null, "pers", "PERSON")) {

            int cnt = 0;

            while (rs.next()) {
                assert "_KEY".equals(rs.getString("COLUMN_NAME"));

                cnt++;
            }

            assert cnt == 1;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllPrimaryKeys() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, null);

            Set<String> expectedPks = new HashSet<>(Arrays.asList(
                "org.ORGANIZATION.PK_org_ORGANIZATION._KEY",
                "pers.PERSON.PK_pers_PERSON._KEY",
                "dep.DEPARTMENT.PK_dep_DEPARTMENT._KEY",
                "PUBLIC.TEST.PK_PUBLIC_TEST.ID",
                "PUBLIC.TEST.PK_PUBLIC_TEST.NAME",
                "PUBLIC.Quoted.PK_PUBLIC_Quoted.Id",
                "PUBLIC.TEST_DECIMAL_COLUMN.ID._KEY"));

            Set<String> actualPks = new HashSet<>(expectedPks.size());

            while(rs.next()) {
                actualPks.add(rs.getString("TABLE_SCHEM") +
                    '.' + rs.getString("TABLE_NAME") +
                    '.' + rs.getString("PK_NAME") +
                    '.' + rs.getString("COLUMN_NAME"));
            }

            assert expectedPks.equals(actualPks) : "expectedPks=" + expectedPks +
                ", actualPks" + actualPks;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testParametersMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setSchema("\"pers\"");

            PreparedStatement stmt = conn.prepareStatement("select orgId from Person p where p.name > ? and p.orgId > ?");

            ParameterMetaData meta = stmt.getParameterMetaData();

            assert meta != null;

            assert meta.getParameterCount() == 2;

            assert meta.getParameterType(1) == Types.VARCHAR;
            assert meta.isNullable(1) == ParameterMetaData.parameterNullableUnknown;
            assert meta.getPrecision(1) == Integer.MAX_VALUE;

            assert meta.getParameterType(2) == Types.INTEGER;
            assert meta.isNullable(2) == ParameterMetaData.parameterNullableUnknown;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSchemasMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getSchemas();

            Set<String> expectedSchemas = new HashSet<>(Arrays.asList("PUBLIC", "pers", "org", "dep"));

            Set<String> schemas = new HashSet<>();

            while (rs.next()) {
                schemas.add(rs.getString(1));

                assert rs.getString(2) == null;
            }

            assert expectedSchemas.equals(schemas) : "Unexpected schemas: " + schemas +
                ". Expected schemas: " + expectedSchemas;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEmptySchemasMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getSchemas(null, "qqq");

            assert !rs.next() : "Empty result set is expected";
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testVersions() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assert conn.getMetaData().getDatabaseProductVersion().equals(IgniteVersionUtils.VER.toString());
            assert conn.getMetaData().getDriverVersion().equals(IgniteVersionUtils.VER.toString());
        }
    }

    /**
     * Person.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Person implements Serializable {
        /** Name. */
        private final String name;

        /** Age. */
        private final int age;

        /** Organization ID. */
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
        private final int id;

        /** Name. */
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

    /**
     * Organization.
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Department implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** Name. */
        @QuerySqlField(precision = 43)
        private final String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        private Department(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
