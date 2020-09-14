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
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
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
                new QueryEntity(AffinityKey.class, Organization.class))),

            cacheConfiguration("metaTest").setQueryEntities(Arrays.asList(
                new QueryEntity(AffinityKey.class, MetaTest.class))));

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        cfg.setSqlConfiguration(new SqlConfiguration().setSqlSchemas("PREDEFINED_SCHEMAS_1", "PREDEFINED_SCHEMAS_2"));

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

        jcache(grid(0),
            defaultCacheConfiguration().setIndexedTypes(Integer.class, Department.class),
            "dep");

        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE PUBLIC.TEST (ID INT, NAME VARCHAR(50) default 'default name', " +
                "age int default 21, VAL VARCHAR(50), PRIMARY KEY (ID, NAME))");
            stmt.execute("CREATE TABLE PUBLIC.\"Quoted\" (\"Id\" INT primary key, \"Name\" VARCHAR(50)) WITH WRAP_KEY");
            stmt.execute("CREATE INDEX \"MyTestIndex quoted\" on PUBLIC.\"Quoted\" (\"Id\" DESC)");
            stmt.execute("CREATE INDEX IDX ON PUBLIC.TEST (ID ASC)");
            stmt.execute("CREATE TABLE PUBLIC.TEST_DECIMAL_COLUMN (ID INT primary key, DEC_COL DECIMAL(8, 3))");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
     * Check that meta data of PreparedStatement and ResultSet's one are equal.
     */
    @Test
    public void testPreparedStatementMetaData() throws Exception {
        // Perform checks few times due to query/plan caching.
        for (int i = 0; i < 3; i++) {
            try (Connection conn = DriverManager.getConnection(BASE_URL)) {
                String select = "select p.name, o.id as orgId from \"pers\".Person p, \"org\".Organization o where p.orgId = o.id";

                ResultSetMetaData rsMeta = conn.createStatement().executeQuery(select).getMetaData();
                ResultSetMetaData psMeta = conn.prepareStatement(select).getMetaData();

                assertEquals(rsMeta.getColumnCount(), rsMeta.getColumnCount());

                for (int j = 1; j <= rsMeta.getColumnCount(); j++) {
                    assertEquals(rsMeta.getTableName(j), psMeta.getTableName(j));
                    assertEquals(rsMeta.getColumnName(j), psMeta.getColumnName(j));
                    assertEquals(rsMeta.getColumnLabel(j), psMeta.getColumnLabel(j));
                    assertEquals(rsMeta.getColumnType(j), psMeta.getColumnType(j));
                    assertEquals(rsMeta.getColumnTypeName(j), psMeta.getColumnTypeName(j));
                    assertEquals(rsMeta.getColumnClassName(j), psMeta.getColumnClassName(j));
                }
            }
        }
    }

    /**
     * Check that non-select statements have null metadata.
     */
    @Test
    public void testPreparedStatementMetaDataNegative() throws Exception {
        // Perform checks few times due to query/plan caching.
        for (int i = 0; i < 3; i++) {
            // Check h2 dml statement.
            try (Connection conn = DriverManager.getConnection(BASE_URL)) {
                String update = "update \"pers\".Person set name = 'weird' where orgId < 0";

                ResultSetMetaData psMeta = conn.prepareStatement(update).getMetaData();

                assertNull(psMeta);
            }

            // H2 ddl statement.
            try (Connection conn = DriverManager.getConnection(BASE_URL)) {
                String update = "CREATE TABLE DDL_METADATA_TAB (ID INT PRIMARY KEY, VAL INT)";

                ResultSetMetaData psMeta = conn.prepareStatement(update).getMetaData();

                assertNull(psMeta);
            }

            // And native parser statement.
            try (Connection conn = DriverManager.getConnection(BASE_URL)) {
                String nativeCmd = "create index my_idx on PUBLIC.TEST(name)";

                ResultSetMetaData psMeta = conn.prepareStatement(nativeCmd).getMetaData();

                assertNull(psMeta);
            }

            // Non parsable.
            try (Connection conn = DriverManager.getConnection(BASE_URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement notCorrect = conn.prepareStatement("select * from NotExistingTable;");

                GridTestUtils.assertThrows(log(), notCorrect::getMetaData, SQLException.class,
                    "Table \"NOTEXISTINGTABLE\" not found");
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDecimalAndDateTypeMetaData() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(
                    "select t.decimal, t.date from \"metaTest\".MetaTest as t");

            assert rs != null;

            ResultSetMetaData meta = rs.getMetaData();

            assert meta != null;

            assert meta.getColumnCount() == 2;

            assert "METATEST".equalsIgnoreCase(meta.getTableName(1));
            assert "DECIMAL".equalsIgnoreCase(meta.getColumnName(1));
            assert "DECIMAL".equalsIgnoreCase(meta.getColumnLabel(1));
            assert meta.getColumnType(1) == DECIMAL;
            assert "DECIMAL".equals(meta.getColumnTypeName(1));
            assert "java.math.BigDecimal".equals(meta.getColumnClassName(1));

            assert "METATEST".equalsIgnoreCase(meta.getTableName(2));
            assert "DATE".equalsIgnoreCase(meta.getColumnName(2));
            assert "DATE".equalsIgnoreCase(meta.getColumnLabel(2));
            assert meta.getColumnType(2) == DATE;
            assert "DATE".equals(meta.getColumnTypeName(2));
            assert "java.sql.Date".equals(meta.getColumnClassName(2));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetTableTypes() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getTableTypes();

            assertTrue(rs.next());

            assertEquals("TABLE", rs.getString("TABLE_TYPE"));

            assertTrue(rs.next());

            assertEquals("VIEW", rs.getString("TABLE_TYPE"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllView() throws Exception {
        Set<String> expViews = new HashSet<>(Arrays.asList(
            "BASELINE_NODES",
            "CACHES",
            "CACHE_GROUPS",
            "INDEXES",
            "LOCAL_CACHE_GROUPS_IO",
            "SQL_QUERIES_HISTORY",
            "SQL_QUERIES",
            "SCAN_QUERIES",
            "NODES",
            "NODE_ATTRIBUTES",
            "NODE_METRICS",
            "SCHEMAS",
            "TABLES",
            "TASKS",
            "JOBS",
            "SERVICES",
            "CLIENT_CONNECTIONS",
            "TRANSACTIONS",
            "VIEWS",
            "TABLE_COLUMNS",
            "VIEW_COLUMNS",
            "CONTINUOUS_QUERIES",
            "STRIPED_THREADPOOL_QUEUE",
            "DATASTREAM_THREADPOOL_QUEUE",
            "CACHE_GROUP_PAGE_LISTS",
            "PARTITION_STATES",
            "BINARY_METADATA",
            "DISTRIBUTED_METASTORAGE"
        ));

        Set<String> actViews = new HashSet<>();

        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getTables(null, null, "%", new String[]{"VIEW"});

            while (rs.next()) {
                assertEquals("VIEW", rs.getString("TABLE_TYPE"));

                assertEquals(JdbcUtils.CATALOG_NAME, rs.getString("TABLE_CAT"));

                assertEquals(QueryUtils.SCHEMA_SYS, rs.getString("TABLE_SCHEM"));

                actViews.add(rs.getString("TABLE_NAME"));
            }

            assertFalse(rs.next());

            assertEquals(expViews, actViews);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetTables() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getTables(null, "pers", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals(JdbcUtils.CATALOG_NAME, rs.getString("TABLE_CAT"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            rs = meta.getTables(null, "org", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals(JdbcUtils.CATALOG_NAME, rs.getString("TABLE_CAT"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            rs = meta.getTables(null, "pers", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals(JdbcUtils.CATALOG_NAME, rs.getString("TABLE_CAT"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            rs = meta.getTables(null, "org", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals(JdbcUtils.CATALOG_NAME, rs.getString("TABLE_CAT"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            rs = meta.getTables(null, "PUBLIC", "", new String[]{"WRONG"});
            assertFalse(rs.next());
        }
    }

    /**
     * Negative scenarios for catalog name.
     * Perform metadata lookups, that use incorrect catalog names.
     */
    @Test
    public void testCatalogWithNotExistingName() throws SQLException {
        checkNoEntitiesFoundForCatalog("");
        checkNoEntitiesFoundForCatalog("NOT_EXISTING_CATALOG");
    }

    /**
     * Check that lookup in the metadata have been performed using specified catalog name (that is neither {@code null}
     * nor correct catalog name), empty result set is returned.
     *
     * @param invalidCat catalog name that is not either
     */
    private void checkNoEntitiesFoundForCatalog(String invalidCat) throws SQLException {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            // Intention: we set the other arguments that way, the values to have as many results as possible.
            assertIsEmpty(meta.getTables(invalidCat, null, "%", new String[] {"TABLE"}));
            assertIsEmpty(meta.getColumns(invalidCat, null, "%", "%"));
            assertIsEmpty(meta.getColumnPrivileges(invalidCat, "pers", "PERSON", "%"));
            assertIsEmpty(meta.getTablePrivileges(invalidCat, null, "%"));
            assertIsEmpty(meta.getPrimaryKeys(invalidCat, "pers", "PERSON"));
            assertIsEmpty(meta.getImportedKeys(invalidCat, "pers", "PERSON"));
            assertIsEmpty(meta.getExportedKeys(invalidCat, "pers", "PERSON"));
            // meta.getCrossReference(...) doesn't make sense because we don't have FK constraint.
            assertIsEmpty(meta.getIndexInfo(invalidCat, null, "%", false, true));
            assertIsEmpty(meta.getSuperTables(invalidCat, "%", "%"));
            assertIsEmpty(meta.getSchemas(invalidCat, null));
            assertIsEmpty(meta.getPseudoColumns(invalidCat, null, "%", ""));
        }
    }

    /**
     * Check JDBC support flags.
     */
    @Test
    public void testCheckSupports() throws SQLException {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertTrue(meta.supportsANSI92EntryLevelSQL());
            assertTrue(meta.supportsAlterTableWithAddColumn());
            assertTrue(meta.supportsAlterTableWithDropColumn());
            assertTrue(meta.nullPlusNonNullIsNull());
        }
    }

    /**
     * Assert that specified ResultSet contains no rows.
     *
     * @param rs result set to check.
     * @throws SQLException on error.
     */
    private static void assertIsEmpty(ResultSet rs) throws SQLException {
        try {
            boolean empty = !rs.next();

            assertTrue("Result should be empty because invalid catalog is specified.", empty);
        }
        finally {
            rs.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns(null, "pers", "PERSON", "%");

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

            rs = meta.getColumns(null, "org", "ORGANIZATION", "%");

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
    @Test
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
    @Test
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
    @Test
    public void testPrimaryKeyMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, null);

            // TABLE_SCHEM.TABLE_NAME.PK_NAME.COLUMN_NAME
            Set<String> expectedPks = new HashSet<>(Arrays.asList(
                "org.ORGANIZATION.PK_org_ORGANIZATION._KEY",
                "pers.PERSON.PK_pers_PERSON._KEY",
                "dep.DEPARTMENT.PK_dep_DEPARTMENT._KEY",
                "PUBLIC.TEST.PK_PUBLIC_TEST.ID",
                "PUBLIC.TEST.PK_PUBLIC_TEST.NAME",
                "PUBLIC.Quoted.PK_PUBLIC_Quoted.Id",
                "PUBLIC.TEST_DECIMAL_COLUMN.ID.ID",
                "metaTest.METATEST.PK_metaTest_METATEST._KEY"));

            Set<String> actualPks = new HashSet<>(expectedPks.size());

            while (rs.next()) {
                actualPks.add(rs.getString("TABLE_SCHEM") +
                    '.' + rs.getString("TABLE_NAME") +
                    '.' + rs.getString("PK_NAME") +
                    '.' + rs.getString("COLUMN_NAME"));
            }

            assertEquals("Metadata contains unexpected primary keys info.", expectedPks, actualPks);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testParametersMetadata() throws Exception {
        // Perform checks few times due to query/plan caching.
        for (int i = 0; i < 3; i++) {
            // No parameters statement.
            try (Connection conn = DriverManager.getConnection(BASE_URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement noParams = conn.prepareStatement("select * from Person;");
                ParameterMetaData params = noParams.getParameterMetaData();

                assertEquals("Parameters should be empty.", 0, params.getParameterCount());
            }

            // Selects.
            try (Connection conn = DriverManager.getConnection(BASE_URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement selectStmt = conn.prepareStatement("select orgId from Person p where p.name > ? and p.orgId > ?");

                ParameterMetaData meta = selectStmt.getParameterMetaData();

                assertNotNull(meta);

                assertEquals(2, meta.getParameterCount());

                assertEquals(Types.VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(Types.INTEGER, meta.getParameterType(2));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));
            }

            // Updates.
            try (Connection conn = DriverManager.getConnection(BASE_URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement updateStmt = conn.prepareStatement("update Person p set orgId = 42 where p.name > ? and p.orgId > ?");

                ParameterMetaData meta = updateStmt.getParameterMetaData();

                assertNotNull(meta);

                assertEquals(2, meta.getParameterCount());

                assertEquals(Types.VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(Types.INTEGER, meta.getParameterType(2));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));
            }
        }
    }

    /**
     * Check that parameters metadata throws correct exception on non-parsable statement.
     */
    @Test
    public void testParametersMetadataNegative() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            conn.setSchema("\"pers\"");

            PreparedStatement notCorrect = conn.prepareStatement("select * from NotExistingTable;");

            GridTestUtils.assertThrows(log(), notCorrect::getParameterMetaData, SQLException.class,
                "Table \"NOTEXISTINGTABLE\" not found");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSchemasMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(BASE_URL)) {
            ResultSet rs = conn.getMetaData().getSchemas();

            Set<String> expectedSchemas = new HashSet<>(Arrays.asList("pers", "org", "metaTest", "dep", "PUBLIC", "SYS", "PREDEFINED_CLIENT_SCHEMA"));

            Set<String> schemas = new HashSet<>();

            while (rs.next()) {
                schemas.add(rs.getString(1));

                assertEquals("There is only one possible catalog.",
                    JdbcUtils.CATALOG_NAME, rs.getString(2));
            }

            assertEquals(expectedSchemas, schemas);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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

    /**
     * Meta Test.
     */
    private static class MetaTest implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** Date. */
        @QuerySqlField
        private final Date date;

        /** decimal. */
        @QuerySqlField
        private final BigDecimal decimal;

        /**
         * @param id ID.
         * @param date Date.
         */
        private MetaTest(int id, Date date, BigDecimal decimal) {
            this.id = id;
            this.date = date;
            this.decimal = decimal;
        }
    }

    /**
     * Department.
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
