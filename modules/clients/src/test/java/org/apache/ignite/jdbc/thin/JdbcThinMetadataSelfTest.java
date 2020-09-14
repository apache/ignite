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
import java.sql.SQLException;
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
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.jdbc2.JdbcUtils;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.metric.sql.SqlViewMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.INTEGER;
import static java.sql.Types.OTHER;
import static java.sql.Types.VARCHAR;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.SCHEMA_SYS;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.internal.util.lang.GridFunc.asMap;

/**
 * Metadata tests.
 */
public class JdbcThinMetadataSelfTest extends JdbcThinAbstractSelfTest {
    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1/";

    /** URL with partition awareness enabled. */
    public static final String URL_PARTITION_AWARENESS =
        "jdbc:ignite:thin://127.0.0.1:10800..10801?partitionAwareness=true";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(new SqlConfiguration()
                .setSqlSchemas("PREDEFINED_SCHEMAS_1", "PREDEFINED_SCHEMAS_2")
            )
            .setMetricExporterSpi(new SqlViewMetricExporterSpi());
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

        jcache(grid(0),
            defaultCacheConfiguration().setIndexedTypes(Integer.class, Department.class),
            "dep");

        try (Connection conn = DriverManager.getConnection(URL)) {
            Statement stmt = conn.createStatement();

            stmt.execute("CREATE TABLE TEST (ID INT, NAME VARCHAR(50) default 'default name', " +
                "age int default 21, VAL VARCHAR(50), PRIMARY KEY (ID, NAME))");
            stmt.execute("CREATE TABLE \"Quoted\" (\"Id\" INT primary key, \"Name\" VARCHAR(50)) WITH WRAP_KEY");
            stmt.execute("CREATE INDEX \"MyTestIndex quoted\" on \"Quoted\" (\"Id\" DESC)");
            stmt.execute("CREATE INDEX IDX ON TEST (ID ASC)");
            stmt.execute("CREATE TABLE TEST_DECIMAL_COLUMN (ID INT primary key, DEC_COL DECIMAL(8, 3))");
            stmt.execute("CREATE TABLE TEST_DECIMAL_COLUMN_PRECISION (ID INT primary key, DEC_COL DECIMAL(8))");
            stmt.execute("CREATE TABLE TEST_DECIMAL_DATE_COLUMN_META (ID INT primary key, DEC_COL DECIMAL(8), DATE_COL DATE)");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
     * Ensure metadata returned for cache without explicit query entities: <ul>
     *     <li>create a cache with indexed types and without explicit query entities</li>
     *     <li>request metadata for columns from the cache</li>
     *     <li>verify returned result</li>
     * </ul>
     *
     * @throws SQLException
     */
    @Test
    public void testMetadataPresentedForAutogeneratedColumns() throws SQLException {
        final String cacheName = "testCache";

        IgniteCache<?, ?> cache = grid(0).createCache(
            new CacheConfiguration<>(cacheName).setIndexedTypes(Integer.class, String.class)
        );

        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns(null, cacheName, null, null);

            assertTrue(rs.next());
            assertEquals(cacheName, rs.getString(MetadataColumn.TABLE_SCHEMA.columnName()));
            assertEquals(KEY_FIELD_NAME, rs.getString(MetadataColumn.COLUMN_NAME.columnName()));

            assertTrue(rs.next());
            assertEquals(cacheName, rs.getString(MetadataColumn.TABLE_SCHEMA.columnName()));
            assertEquals(VAL_FIELD_NAME, rs.getString(MetadataColumn.COLUMN_NAME.columnName()));

            assertFalse(rs.next());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * Ensure metadata returned for cache with explicit query entities has no duplicate columns: <ul>
     *     <li>create a cache with indexed types and explicit query entity</li>
     *     <li>request metadata for columns from the cache</li>
     *     <li>verify returned result</li>
     * </ul>
     *
     * @throws SQLException
     */
    @Test
    public void testMetadataNotDublicatedIfFieldsSetExplicitly() throws SQLException {
        final String cacheName = "testCache";

        IgniteCache<?, ?> cache = grid(0).createCache(
            new CacheConfiguration<>(cacheName)
                .setQueryEntities(Collections.singleton(
                    new QueryEntity()
                        .setKeyType(Integer.class.getName())
                        .setKeyFieldName("my_key")
                        .setValueType(String.class.getName())
                        .setFields(new LinkedHashMap<>(asMap("my_key", Integer.class.getName())))
                ))
                .setIndexedTypes(Integer.class, String.class)
        );

        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns(null, cacheName, null, null);

            assertTrue(rs.next());
            assertEquals(cacheName, rs.getString(MetadataColumn.TABLE_SCHEMA.columnName()));
            assertEquals("MY_KEY", rs.getString(MetadataColumn.COLUMN_NAME.columnName()));

            assertFalse(rs.next());
        }
        finally {
            cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDecimalAndDateTypeMetaData() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            Statement stmt = conn.createStatement();

            ResultSet rs = stmt.executeQuery(
                    "select t.dec_col, t.date_col from TEST_DECIMAL_DATE_COLUMN_META as t");

            assert rs != null;

            ResultSetMetaData meta = rs.getMetaData();

            assert meta != null;

            assert meta.getColumnCount() == 2;

            assert "TEST_DECIMAL_DATE_COLUMN_META".equalsIgnoreCase(meta.getTableName(1));
            assert "DEC_COL".equalsIgnoreCase(meta.getColumnName(1));
            assert "DEC_COL".equalsIgnoreCase(meta.getColumnLabel(1));
            assert meta.getColumnType(1) == DECIMAL;
            assert "DECIMAL".equals(meta.getColumnTypeName(1));
            assert "java.math.BigDecimal".equals(meta.getColumnClassName(1));

            assert "TEST_DECIMAL_DATE_COLUMN_META".equalsIgnoreCase(meta.getTableName(2));
            assert "DATE_COL".equalsIgnoreCase(meta.getColumnName(2));
            assert "DATE_COL".equalsIgnoreCase(meta.getColumnLabel(2));
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
        try (Connection conn = DriverManager.getConnection(URL)) {
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
    public void testGetTables() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getTables(null, "pers", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals(JdbcUtils.CATALOG_NAME, rs.getString("TABLE_CAT"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            assertFalse(rs.next());

            rs = meta.getTables(null, "org", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals(JdbcUtils.CATALOG_NAME, rs.getString("TABLE_CAT"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            assertFalse(rs.next());

            rs = meta.getTables(null, "pers", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals(JdbcUtils.CATALOG_NAME, rs.getString("TABLE_CAT"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            assertFalse(rs.next());

            rs = meta.getTables(null, "org", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals(JdbcUtils.CATALOG_NAME, rs.getString("TABLE_CAT"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            assertFalse(rs.next());

            rs = meta.getTables(null, "PUBLIC", "", new String[]{"WRONG"});
            assertFalse(rs.next());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllTables() throws Exception {
        testGetTables(
            new String[] {"TABLE"},
            new HashSet<>(Arrays.asList(
                "org.ORGANIZATION",
                "pers.PERSON",
                "dep.DEPARTMENT",
                "PUBLIC.TEST",
                "PUBLIC.Quoted",
                "PUBLIC.TEST_DECIMAL_COLUMN",
                "PUBLIC.TEST_DECIMAL_COLUMN_PRECISION",
                "PUBLIC.TEST_DECIMAL_DATE_COLUMN_META"))
        );
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllView() throws Exception {
        testGetTables(
            new String[] {"VIEW"},
            new HashSet<>(Arrays.asList(
                "SYS.METRICS",
                "SYS.SERVICES",
                "SYS.CACHE_GROUPS",
                "SYS.CACHES",
                "SYS.TASKS",
                "SYS.JOBS",
                "SYS.SQL_QUERIES_HISTORY",
                "SYS.NODES",
                "SYS.SCHEMAS",
                "SYS.NODE_METRICS",
                "SYS.BASELINE_NODES",
                "SYS.INDEXES",
                "SYS.LOCAL_CACHE_GROUPS_IO",
                "SYS.SQL_QUERIES",
                "SYS.SCAN_QUERIES",
                "SYS.NODE_ATTRIBUTES",
                "SYS.TABLES",
                "SYS.CLIENT_CONNECTIONS",
                "SYS.TRANSACTIONS",
                "SYS.VIEWS",
                "SYS.TABLE_COLUMNS",
                "SYS.VIEW_COLUMNS",
                "SYS.CONTINUOUS_QUERIES",
                "SYS.STRIPED_THREADPOOL_QUEUE",
                "SYS.DATASTREAM_THREADPOOL_QUEUE",
                "SYS.CACHE_GROUP_PAGE_LISTS",
                "SYS.DATA_REGION_PAGE_LISTS",
                "SYS.PARTITION_STATES",
                "SYS.BINARY_METADATA",
                "SYS.DISTRIBUTED_METASTORAGE"
            ))
        );
    }

    /**
     * @throws Exception If failed.
     */
    private void testGetTables(String[] tblTypes, Set<String> expTbls) throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getTables(null, null, null, tblTypes);

            Set<String> actualTbls = new HashSet<>(expTbls.size());

            while (rs.next()) {
                actualTbls.add(rs.getString("TABLE_SCHEM") + '.'
                    + rs.getString("TABLE_NAME"));
            }

            assert expTbls.equals(actualTbls) : "expectedTbls=" + expTbls +
                ", actualTbls" + actualTbls;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            conn.setSchema("pers");

            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns(null, "pers", "PERSON", "%");

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

                assert names.remove(name) : "Unexpected column name " + name;

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

            rs = meta.getColumns(null, "org", "ORGANIZATION", "%");

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
    @Test
    public void testGetAllColumns() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            ResultSet rs = meta.getColumns(null, null, null, null);

            Set<String> expectedCols = new HashSet<>(Arrays.asList(
                "PUBLIC.Quoted.Id.null",
                "PUBLIC.Quoted.Name.null.50",
                "PUBLIC.TEST.ID.null",
                "PUBLIC.TEST.NAME.'default name'.50",
                "PUBLIC.TEST.AGE.21",
                "PUBLIC.TEST.VAL.null.50",
                "PUBLIC.TEST_DECIMAL_COLUMN.ID.null",
                "PUBLIC.TEST_DECIMAL_COLUMN.DEC_COL.null.8.3",
                "PUBLIC.TEST_DECIMAL_COLUMN_PRECISION.ID.null",
                "PUBLIC.TEST_DECIMAL_COLUMN_PRECISION.DEC_COL.null.8",
                "PUBLIC.TEST_DECIMAL_DATE_COLUMN_META.ID.null",
                "PUBLIC.TEST_DECIMAL_DATE_COLUMN_META.DEC_COL.null.8",
                "PUBLIC.TEST_DECIMAL_DATE_COLUMN_META.DATE_COL.null",
                "dep.DEPARTMENT.ID.null",
                "dep.DEPARTMENT.NAME.null.43",
                "org.ORGANIZATION.ID.null",
                "org.ORGANIZATION.NAME.null.42",
                "pers.PERSON.NAME.null",
                "pers.PERSON.AGE.null",
                "pers.PERSON.ORGID.null"
            ));

            Set<String> actualUserCols = new HashSet<>(expectedCols.size());

            Set<String> actualSystemCols = new HashSet<>();

            while (rs.next()) {
                int precision = rs.getInt("COLUMN_SIZE");

                int scale = rs.getInt("DECIMAL_DIGITS");

                String schemaName = rs.getString("TABLE_SCHEM");

                String colDefinition = schemaName + '.'
                    + rs.getString("TABLE_NAME") + "."
                    + rs.getString("COLUMN_NAME") + "."
                    + rs.getString("COLUMN_DEF")
                    + (precision == 0 ? "" : ("." + precision))
                    + (scale == 0 ? "" : ("." + scale));

                if (!schemaName.equals(SCHEMA_SYS))
                    actualUserCols.add(colDefinition);
                else
                    actualSystemCols.add(colDefinition);
            }

            Assert.assertEquals(expectedCols, actualUserCols);

            expectedCols = new HashSet<>(Arrays.asList(
                "SYS.BASELINE_NODES.CONSISTENT_ID.null.2147483647",
                "SYS.BASELINE_NODES.ONLINE.null.1",
                "SYS.CACHES.CACHE_GROUP_ID.null.10",
                "SYS.CACHES.CACHE_GROUP_NAME.null.2147483647",
                "SYS.CACHES.CACHE_ID.null.10",
                "SYS.CACHES.CACHE_NAME.null.2147483647",
                "SYS.CACHES.CACHE_TYPE.null.2147483647",
                "SYS.CACHES.CACHE_MODE.null.2147483647",
                "SYS.CACHES.ATOMICITY_MODE.null.2147483647",
                "SYS.CACHES.IS_ONHEAP_CACHE_ENABLED.null.1",
                "SYS.CACHES.IS_COPY_ON_READ.null.1",
                "SYS.CACHES.IS_LOAD_PREVIOUS_VALUE.null.1",
                "SYS.CACHES.IS_READ_FROM_BACKUP.null.1",
                "SYS.CACHES.PARTITION_LOSS_POLICY.null.2147483647",
                "SYS.CACHES.NODE_FILTER.null.2147483647",
                "SYS.CACHES.TOPOLOGY_VALIDATOR.null.2147483647",
                "SYS.CACHES.IS_EAGER_TTL.null.1",
                "SYS.CACHES.WRITE_SYNCHRONIZATION_MODE.null.2147483647",
                "SYS.CACHES.IS_INVALIDATE.null.1",
                "SYS.CACHES.IS_EVENTS_DISABLED.null.1",
                "SYS.CACHES.IS_STATISTICS_ENABLED.null.1",
                "SYS.CACHES.IS_MANAGEMENT_ENABLED.null.1",
                "SYS.CACHES.BACKUPS.null.10",
                "SYS.CACHES.AFFINITY.null.2147483647",
                "SYS.CACHES.AFFINITY_MAPPER.null.2147483647",
                "SYS.CACHES.REBALANCE_MODE.null.2147483647",
                "SYS.CACHES.REBALANCE_BATCH_SIZE.null.10",
                "SYS.CACHES.REBALANCE_TIMEOUT.null.19",
                "SYS.CACHES.REBALANCE_DELAY.null.19",
                "SYS.CACHES.REBALANCE_THROTTLE.null.19",
                "SYS.CACHES.REBALANCE_BATCHES_PREFETCH_COUNT.null.19",
                "SYS.CACHES.REBALANCE_ORDER.null.10",
                "SYS.CACHES.EVICTION_FILTER.null.2147483647",
                "SYS.CACHES.EVICTION_POLICY_FACTORY.null.2147483647",
                "SYS.CACHES.IS_NEAR_CACHE_ENABLED.null.1",
                "SYS.CACHES.NEAR_CACHE_EVICTION_POLICY_FACTORY.null.2147483647",
                "SYS.CACHES.NEAR_CACHE_START_SIZE.null.10",
                "SYS.CACHES.DEFAULT_LOCK_TIMEOUT.null.19",
                "SYS.CACHES.INTERCEPTOR.null.2147483647",
                "SYS.CACHES.CACHE_STORE_FACTORY.null.2147483647",
                "SYS.CACHES.IS_STORE_KEEP_BINARY.null.1",
                "SYS.CACHES.IS_READ_THROUGH.null.1",
                "SYS.CACHES.IS_WRITE_THROUGH.null.1",
                "SYS.CACHES.IS_WRITE_BEHIND_ENABLED.null.1",
                "SYS.CACHES.WRITE_BEHIND_COALESCING.null.1",
                "SYS.CACHES.WRITE_BEHIND_FLUSH_SIZE.null.10",
                "SYS.CACHES.WRITE_BEHIND_FLUSH_FREQUENCY.null.19",
                "SYS.CACHES.WRITE_BEHIND_FLUSH_THREAD_COUNT.null.10",
                "SYS.CACHES.WRITE_BEHIND_BATCH_SIZE.null.10",
                "SYS.CACHES.MAX_CONCURRENT_ASYNC_OPERATIONS.null.10",
                "SYS.CACHES.CACHE_LOADER_FACTORY.null.2147483647",
                "SYS.CACHES.CACHE_WRITER_FACTORY.null.2147483647",
                "SYS.CACHES.EXPIRY_POLICY_FACTORY.null.2147483647",
                "SYS.CACHES.IS_SQL_ESCAPE_ALL.null.1",
                "SYS.CACHES.IS_ENCRYPTION_ENABLED.null.1",
                "SYS.CACHES.SQL_SCHEMA.null.2147483647",
                "SYS.CACHES.SQL_INDEX_MAX_INLINE_SIZE.null.10",
                "SYS.CACHES.IS_SQL_ONHEAP_CACHE_ENABLED.null.1",
                "SYS.CACHES.SQL_ONHEAP_CACHE_MAX_SIZE.null.10",
                "SYS.CACHES.QUERY_DETAIL_METRICS_SIZE.null.10",
                "SYS.CACHES.QUERY_PARALLELISM.null.10",
                "SYS.CACHES.MAX_QUERY_ITERATORS_COUNT.null.10",
                "SYS.CACHES.DATA_REGION_NAME.null.2147483647",
                "SYS.CACHE_GROUPS.CACHE_GROUP_ID.null.10",
                "SYS.CACHE_GROUPS.CACHE_GROUP_NAME.null.2147483647",
                "SYS.CACHE_GROUPS.IS_SHARED.null.1",
                "SYS.CACHE_GROUPS.CACHE_COUNT.null.10",
                "SYS.CACHE_GROUPS.CACHE_MODE.null.2147483647",
                "SYS.CACHE_GROUPS.ATOMICITY_MODE.null.2147483647",
                "SYS.CACHE_GROUPS.AFFINITY.null.2147483647",
                "SYS.CACHE_GROUPS.PARTITIONS_COUNT.null.10",
                "SYS.CACHE_GROUPS.NODE_FILTER.null.2147483647",
                "SYS.CACHE_GROUPS.DATA_REGION_NAME.null.2147483647",
                "SYS.CACHE_GROUPS.TOPOLOGY_VALIDATOR.null.2147483647",
                "SYS.CACHE_GROUPS.PARTITION_LOSS_POLICY.null.2147483647",
                "SYS.CACHE_GROUPS.REBALANCE_MODE.null.2147483647",
                "SYS.CACHE_GROUPS.REBALANCE_DELAY.null.19",
                "SYS.CACHE_GROUPS.REBALANCE_ORDER.null.10",
                "SYS.CACHE_GROUPS.BACKUPS.null.10",
                "SYS.INDEXES.CACHE_ID.null.10",
                "SYS.INDEXES.CACHE_NAME.null.2147483647",
                "SYS.INDEXES.SCHEMA_NAME.null.2147483647",
                "SYS.INDEXES.TABLE_NAME.null.2147483647",
                "SYS.INDEXES.INDEX_NAME.null.2147483647",
                "SYS.INDEXES.INDEX_TYPE.null.2147483647",
                "SYS.INDEXES.COLUMNS.null.2147483647",
                "SYS.INDEXES.IS_PK.null.1",
                "SYS.INDEXES.IS_UNIQUE.null.1",
                "SYS.INDEXES.INLINE_SIZE.null.10",
                "SYS.LOCAL_CACHE_GROUPS_IO.CACHE_GROUP_ID.null.10",
                "SYS.LOCAL_CACHE_GROUPS_IO.CACHE_GROUP_NAME.null.2147483647",
                "SYS.LOCAL_CACHE_GROUPS_IO.PHYSICAL_READS.null.19",
                "SYS.LOCAL_CACHE_GROUPS_IO.LOGICAL_READS.null.19",
                "SYS.SQL_QUERIES_HISTORY.SCHEMA_NAME.null.2147483647",
                "SYS.SQL_QUERIES_HISTORY.SQL.null.2147483647",
                "SYS.SQL_QUERIES_HISTORY.LOCAL.null.1",
                "SYS.SQL_QUERIES_HISTORY.EXECUTIONS.null.19",
                "SYS.SQL_QUERIES_HISTORY.FAILURES.null.19",
                "SYS.SQL_QUERIES_HISTORY.DURATION_MIN.null.19",
                "SYS.SQL_QUERIES_HISTORY.DURATION_MAX.null.19",
                "SYS.SQL_QUERIES_HISTORY.LAST_START_TIME.null.26.6",
                "SYS.SQL_QUERIES.QUERY_ID.null.2147483647",
                "SYS.SQL_QUERIES.SQL.null.2147483647",
                "SYS.SQL_QUERIES.SCHEMA_NAME.null.2147483647",
                "SYS.SQL_QUERIES.LOCAL.null.1",
                "SYS.SQL_QUERIES.START_TIME.null.26.6",
                "SYS.SQL_QUERIES.DURATION.null.19",
                "SYS.SQL_QUERIES.ORIGIN_NODE_ID.null.2147483647",
                "SYS.SCAN_QUERIES.START_TIME.null.19",
                "SYS.SCAN_QUERIES.TRANSFORMER.null.2147483647",
                "SYS.SCAN_QUERIES.LOCAL.null.1",
                "SYS.SCAN_QUERIES.QUERY_ID.null.19",
                "SYS.SCAN_QUERIES.PARTITION.null.10",
                "SYS.SCAN_QUERIES.CACHE_GROUP_ID.null.10",
                "SYS.SCAN_QUERIES.CACHE_NAME.null.2147483647",
                "SYS.SCAN_QUERIES.TOPOLOGY.null.2147483647",
                "SYS.SCAN_QUERIES.CACHE_GROUP_NAME.null.2147483647",
                "SYS.SCAN_QUERIES.TASK_NAME.null.2147483647",
                "SYS.SCAN_QUERIES.DURATION.null.19",
                "SYS.SCAN_QUERIES.KEEP_BINARY.null.1",
                "SYS.SCAN_QUERIES.FILTER.null.2147483647",
                "SYS.SCAN_QUERIES.SUBJECT_ID.null.2147483647",
                "SYS.SCAN_QUERIES.CANCELED.null.1",
                "SYS.SCAN_QUERIES.CACHE_ID.null.10",
                "SYS.SCAN_QUERIES.PAGE_SIZE.null.10",
                "SYS.SCAN_QUERIES.ORIGIN_NODE_ID.null.2147483647",
                "SYS.NODES.NODE_ID.null.2147483647",
                "SYS.NODES.CONSISTENT_ID.null.2147483647",
                "SYS.NODES.VERSION.null.2147483647",
                "SYS.NODES.IS_CLIENT.null.1",
                "SYS.NODES.IS_DAEMON.null.1",
                "SYS.NODES.IS_LOCAL.null.1",
                "SYS.NODES.NODE_ORDER.null.19",
                "SYS.NODES.ADDRESSES.null.2147483647",
                "SYS.NODES.HOSTNAMES.null.2147483647",
                "SYS.NODE_ATTRIBUTES.NODE_ID.null.2147483647",
                "SYS.NODE_ATTRIBUTES.NAME.null.2147483647",
                "SYS.NODE_ATTRIBUTES.VALUE.null.2147483647",
                "SYS.NODE_METRICS.NODE_ID.null.2147483647",
                "SYS.NODE_METRICS.LAST_UPDATE_TIME.null.26.6",
                "SYS.NODE_METRICS.MAX_ACTIVE_JOBS.null.10",
                "SYS.NODE_METRICS.CUR_ACTIVE_JOBS.null.10",
                "SYS.NODE_METRICS.AVG_ACTIVE_JOBS.null.7",
                "SYS.NODE_METRICS.MAX_WAITING_JOBS.null.10",
                "SYS.NODE_METRICS.CUR_WAITING_JOBS.null.10",
                "SYS.NODE_METRICS.AVG_WAITING_JOBS.null.7",
                "SYS.NODE_METRICS.MAX_REJECTED_JOBS.null.10",
                "SYS.NODE_METRICS.CUR_REJECTED_JOBS.null.10",
                "SYS.NODE_METRICS.AVG_REJECTED_JOBS.null.7",
                "SYS.NODE_METRICS.TOTAL_REJECTED_JOBS.null.10",
                "SYS.NODE_METRICS.MAX_CANCELED_JOBS.null.10",
                "SYS.NODE_METRICS.CUR_CANCELED_JOBS.null.10",
                "SYS.NODE_METRICS.AVG_CANCELED_JOBS.null.7",
                "SYS.NODE_METRICS.TOTAL_CANCELED_JOBS.null.10",
                "SYS.NODE_METRICS.MAX_JOBS_WAIT_TIME.null.19",
                "SYS.NODE_METRICS.CUR_JOBS_WAIT_TIME.null.19",
                "SYS.NODE_METRICS.AVG_JOBS_WAIT_TIME.null.19",
                "SYS.NODE_METRICS.MAX_JOBS_EXECUTE_TIME.null.19",
                "SYS.NODE_METRICS.CUR_JOBS_EXECUTE_TIME.null.19",
                "SYS.NODE_METRICS.AVG_JOBS_EXECUTE_TIME.null.19",
                "SYS.NODE_METRICS.TOTAL_JOBS_EXECUTE_TIME.null.19",
                "SYS.NODE_METRICS.TOTAL_EXECUTED_JOBS.null.10",
                "SYS.NODE_METRICS.TOTAL_EXECUTED_TASKS.null.10",
                "SYS.NODE_METRICS.TOTAL_BUSY_TIME.null.19",
                "SYS.NODE_METRICS.TOTAL_IDLE_TIME.null.19",
                "SYS.NODE_METRICS.CUR_IDLE_TIME.null.19",
                "SYS.NODE_METRICS.BUSY_TIME_PERCENTAGE.null.7",
                "SYS.NODE_METRICS.IDLE_TIME_PERCENTAGE.null.7",
                "SYS.NODE_METRICS.TOTAL_CPU.null.10",
                "SYS.NODE_METRICS.CUR_CPU_LOAD.null.17",
                "SYS.NODE_METRICS.AVG_CPU_LOAD.null.17",
                "SYS.NODE_METRICS.CUR_GC_CPU_LOAD.null.17",
                "SYS.NODE_METRICS.HEAP_MEMORY_INIT.null.19",
                "SYS.NODE_METRICS.HEAP_MEMORY_USED.null.19",
                "SYS.NODE_METRICS.HEAP_MEMORY_COMMITED.null.19",
                "SYS.NODE_METRICS.HEAP_MEMORY_MAX.null.19",
                "SYS.NODE_METRICS.HEAP_MEMORY_TOTAL.null.19",
                "SYS.NODE_METRICS.NONHEAP_MEMORY_INIT.null.19",
                "SYS.NODE_METRICS.NONHEAP_MEMORY_USED.null.19",
                "SYS.NODE_METRICS.NONHEAP_MEMORY_COMMITED.null.19",
                "SYS.NODE_METRICS.NONHEAP_MEMORY_MAX.null.19",
                "SYS.NODE_METRICS.NONHEAP_MEMORY_TOTAL.null.19",
                "SYS.NODE_METRICS.UPTIME.null.19",
                "SYS.NODE_METRICS.JVM_START_TIME.null.26.6",
                "SYS.NODE_METRICS.NODE_START_TIME.null.26.6",
                "SYS.NODE_METRICS.LAST_DATA_VERSION.null.19",
                "SYS.NODE_METRICS.CUR_THREAD_COUNT.null.10",
                "SYS.NODE_METRICS.MAX_THREAD_COUNT.null.10",
                "SYS.NODE_METRICS.TOTAL_THREAD_COUNT.null.19",
                "SYS.NODE_METRICS.CUR_DAEMON_THREAD_COUNT.null.10",
                "SYS.NODE_METRICS.SENT_MESSAGES_COUNT.null.10",
                "SYS.NODE_METRICS.SENT_BYTES_COUNT.null.19",
                "SYS.NODE_METRICS.RECEIVED_MESSAGES_COUNT.null.10",
                "SYS.NODE_METRICS.RECEIVED_BYTES_COUNT.null.19",
                "SYS.NODE_METRICS.OUTBOUND_MESSAGES_QUEUE.null.10",
                "SYS.TABLES.CACHE_ID.null.10",
                "SYS.TABLES.CACHE_NAME.null.2147483647",
                "SYS.TABLES.SCHEMA_NAME.null.2147483647",
                "SYS.TABLES.TABLE_NAME.null.2147483647",
                "SYS.TABLES.AFFINITY_KEY_COLUMN.null.2147483647",
                "SYS.TABLES.KEY_ALIAS.null.2147483647",
                "SYS.TABLES.VALUE_ALIAS.null.2147483647",
                "SYS.TABLES.KEY_TYPE_NAME.null.2147483647",
                "SYS.TABLES.VALUE_TYPE_NAME.null.2147483647",
                "SYS.TABLES.IS_INDEX_REBUILD_IN_PROGRESS.null.1",
                "SYS.METRICS.NAME.null.2147483647",
                "SYS.METRICS.VALUE.null.2147483647",
                "SYS.METRICS.DESCRIPTION.null.2147483647",
                "SYS.SERVICES.SERVICE_ID.null.2147483647",
                "SYS.SERVICES.NAME.null.2147483647",
                "SYS.SERVICES.SERVICE_CLASS.null.2147483647",
                "SYS.SERVICES.CACHE_NAME.null.2147483647",
                "SYS.SERVICES.ORIGIN_NODE_ID.null.2147483647",
                "SYS.SERVICES.TOTAL_COUNT.null.10",
                "SYS.SERVICES.MAX_PER_NODE_COUNT.null.10",
                "SYS.SERVICES.AFFINITY_KEY.null.2147483647",
                "SYS.SERVICES.NODE_FILTER.null.2147483647",
                "SYS.SERVICES.STATICALLY_CONFIGURED.null.1",
                "SYS.SERVICES.SERVICE_ID.null.2147483647",
                "SYS.TASKS.AFFINITY_CACHE_NAME.null.2147483647",
                "SYS.TASKS.INTERNAL.null.1",
                "SYS.TASKS.END_TIME.null.19",
                "SYS.TASKS.START_TIME.null.19",
                "SYS.TASKS.USER_VERSION.null.2147483647",
                "SYS.TASKS.TASK_NAME.null.2147483647",
                "SYS.TASKS.TASK_NODE_ID.null.2147483647",
                "SYS.TASKS.JOB_ID.null.2147483647",
                "SYS.TASKS.ID.null.2147483647",
                "SYS.TASKS.SESSION_ID.null.2147483647",
                "SYS.TASKS.AFFINITY_PARTITION_ID.null.10",
                "SYS.TASKS.TASK_CLASS_NAME.null.2147483647",
                "SYS.JOBS.IS_STARTED.null.1",
                "SYS.JOBS.EXECUTOR_NAME.null.2147483647",
                "SYS.JOBS.IS_TIMED_OUT.null.1",
                "SYS.JOBS.ID.null.2147483647",
                "SYS.JOBS.FINISH_TIME.null.19",
                "SYS.JOBS.IS_INTERNAL.null.1",
                "SYS.JOBS.CREATE_TIME.null.19",
                "SYS.JOBS.AFFINITY_PARTITION_ID.null.10",
                "SYS.JOBS.ORIGIN_NODE_ID.null.2147483647",
                "SYS.JOBS.TASK_NAME.null.2147483647",
                "SYS.JOBS.TASK_CLASS_NAME.null.2147483647",
                "SYS.JOBS.SESSION_ID.null.2147483647",
                "SYS.JOBS.IS_FINISHING.null.1",
                "SYS.JOBS.START_TIME.null.19",
                "SYS.JOBS.AFFINITY_CACHE_IDS.null.2147483647",
                "SYS.JOBS.STATE.null.2147483647",
                "SYS.CLIENT_CONNECTIONS.CONNECTION_ID.null.19",
                "SYS.CLIENT_CONNECTIONS.LOCAL_ADDRESS.null.2147483647",
                "SYS.CLIENT_CONNECTIONS.REMOTE_ADDRESS.null.2147483647",
                "SYS.CLIENT_CONNECTIONS.TYPE.null.2147483647",
                "SYS.CLIENT_CONNECTIONS.USER.null.2147483647",
                "SYS.CLIENT_CONNECTIONS.VERSION.null.2147483647",
                "SYS.TASKS.EXEC_NAME.null.2147483647",
                "SYS.TRANSACTIONS.LOCAL_NODE_ID.null.2147483647",
                "SYS.TRANSACTIONS.STATE.null.2147483647",
                "SYS.TRANSACTIONS.XID.null.2147483647",
                "SYS.TRANSACTIONS.LABEL.null.2147483647",
                "SYS.TRANSACTIONS.START_TIME.null.19",
                "SYS.TRANSACTIONS.ISOLATION.null.2147483647",
                "SYS.TRANSACTIONS.CONCURRENCY.null.2147483647",
                "SYS.TRANSACTIONS.COLOCATED.null.1",
                "SYS.TRANSACTIONS.DHT.null.1",
                "SYS.TRANSACTIONS.IMPLICIT.null.1",
                "SYS.TRANSACTIONS.IMPLICIT_SINGLE.null.1",
                "SYS.TRANSACTIONS.INTERNAL.null.1",
                "SYS.TRANSACTIONS.LOCAL.null.1",
                "SYS.TRANSACTIONS.NEAR.null.1",
                "SYS.TRANSACTIONS.ONE_PHASE_COMMIT.null.1",
                "SYS.TRANSACTIONS.SUBJECT_ID.null.2147483647",
                "SYS.TRANSACTIONS.SYSTEM.null.1",
                "SYS.TRANSACTIONS.THREAD_ID.null.19",
                "SYS.TRANSACTIONS.TIMEOUT.null.19",
                "SYS.TRANSACTIONS.DURATION.null.19",
                "SYS.TRANSACTIONS.ORIGINATING_NODE_ID.null.2147483647",
                "SYS.TRANSACTIONS.OTHER_NODE_ID.null.2147483647",
                "SYS.TRANSACTIONS.TOP_VER.null.2147483647",
                "SYS.TRANSACTIONS.KEYS_COUNT.null.10",
                "SYS.TRANSACTIONS.CACHE_IDS.null.2147483647",
                "SYS.SCHEMAS.NAME.null.2147483647",
                "SYS.SCHEMAS.PREDEFINED.null.1",
                "SYS.VIEWS.NAME.null.2147483647",
                "SYS.VIEWS.DESCRIPTION.null.2147483647",
                "SYS.VIEWS.SCHEMA.null.2147483647",
                "SYS.TABLE_COLUMNS.AFFINITY_COLUMN.null.1",
                "SYS.TABLE_COLUMNS.COLUMN_NAME.null.2147483647",
                "SYS.TABLE_COLUMNS.SCALE.null.10",
                "SYS.TABLE_COLUMNS.PK.null.1",
                "SYS.TABLE_COLUMNS.TYPE.null.2147483647",
                "SYS.TABLE_COLUMNS.DEFAULT_VALUE.null.2147483647",
                "SYS.TABLE_COLUMNS.SCHEMA_NAME.null.2147483647",
                "SYS.TABLE_COLUMNS.TABLE_NAME.null.2147483647",
                "SYS.TABLE_COLUMNS.NULLABLE.null.1",
                "SYS.TABLE_COLUMNS.PRECISION.null.10",
                "SYS.TABLE_COLUMNS.AUTO_INCREMENT.null.1",
                "SYS.VIEW_COLUMNS.NULLABLE.null.1",
                "SYS.VIEW_COLUMNS.SCHEMA_NAME.null.2147483647",
                "SYS.VIEW_COLUMNS.COLUMN_NAME.null.2147483647",
                "SYS.VIEW_COLUMNS.TYPE.null.2147483647",
                "SYS.VIEW_COLUMNS.PRECISION.null.19",
                "SYS.VIEW_COLUMNS.DEFAULT_VALUE.null.2147483647",
                "SYS.VIEW_COLUMNS.SCALE.null.10",
                "SYS.VIEW_COLUMNS.VIEW_NAME.null.2147483647",
                "SYS.CONTINUOUS_QUERIES.NOTIFY_EXISTING.null.1",
                "SYS.CONTINUOUS_QUERIES.OLD_VALUE_REQUIRED.null.1",
                "SYS.CONTINUOUS_QUERIES.KEEP_BINARY.null.1",
                "SYS.CONTINUOUS_QUERIES.IS_MESSAGING.null.1",
                "SYS.CONTINUOUS_QUERIES.AUTO_UNSUBSCRIBE.null.1",
                "SYS.CONTINUOUS_QUERIES.LAST_SEND_TIME.null.19",
                "SYS.CONTINUOUS_QUERIES.LOCAL_TRANSFORMED_LISTENER.null.2147483647",
                "SYS.CONTINUOUS_QUERIES.TOPIC.null.2147483647",
                "SYS.CONTINUOUS_QUERIES.BUFFER_SIZE.null.10",
                "SYS.CONTINUOUS_QUERIES.REMOTE_TRANSFORMER.null.2147483647",
                "SYS.CONTINUOUS_QUERIES.DELAYED_REGISTER.null.1",
                "SYS.CONTINUOUS_QUERIES.IS_QUERY.null.1",
                "SYS.CONTINUOUS_QUERIES.NODE_ID.null.2147483647",
                "SYS.CONTINUOUS_QUERIES.INTERVAL.null.19",
                "SYS.CONTINUOUS_QUERIES.IS_EVENTS.null.1",
                "SYS.CONTINUOUS_QUERIES.ROUTINE_ID.null.2147483647",
                "SYS.CONTINUOUS_QUERIES.REMOTE_FILTER.null.2147483647",
                "SYS.CONTINUOUS_QUERIES.CACHE_NAME.null.2147483647",
                "SYS.CONTINUOUS_QUERIES.LOCAL_LISTENER.null.2147483647",
                "SYS.STRIPED_THREADPOOL_QUEUE.STRIPE_INDEX.null.10",
                "SYS.STRIPED_THREADPOOL_QUEUE.DESCRIPTION.null.2147483647",
                "SYS.STRIPED_THREADPOOL_QUEUE.THREAD_NAME.null.2147483647",
                "SYS.STRIPED_THREADPOOL_QUEUE.TASK_NAME.null.2147483647",
                "SYS.DATASTREAM_THREADPOOL_QUEUE.STRIPE_INDEX.null.10",
                "SYS.DATASTREAM_THREADPOOL_QUEUE.DESCRIPTION.null.2147483647",
                "SYS.DATASTREAM_THREADPOOL_QUEUE.THREAD_NAME.null.2147483647",
                "SYS.DATASTREAM_THREADPOOL_QUEUE.TASK_NAME.null.2147483647",
                "SYS.CACHE_GROUP_PAGE_LISTS.CACHE_GROUP_ID.null.10",
                "SYS.CACHE_GROUP_PAGE_LISTS.PARTITION_ID.null.10",
                "SYS.CACHE_GROUP_PAGE_LISTS.NAME.null.2147483647",
                "SYS.CACHE_GROUP_PAGE_LISTS.BUCKET_NUMBER.null.10",
                "SYS.CACHE_GROUP_PAGE_LISTS.BUCKET_SIZE.null.19",
                "SYS.CACHE_GROUP_PAGE_LISTS.STRIPES_COUNT.null.10",
                "SYS.CACHE_GROUP_PAGE_LISTS.CACHED_PAGES_COUNT.null.10",
                "SYS.DATA_REGION_PAGE_LISTS.NAME.null.2147483647",
                "SYS.DATA_REGION_PAGE_LISTS.BUCKET_NUMBER.null.10",
                "SYS.DATA_REGION_PAGE_LISTS.BUCKET_SIZE.null.19",
                "SYS.DATA_REGION_PAGE_LISTS.STRIPES_COUNT.null.10",
                "SYS.DATA_REGION_PAGE_LISTS.CACHED_PAGES_COUNT.null.10",
                "SYS.PARTITION_STATES.CACHE_GROUP_ID.null.10",
                "SYS.PARTITION_STATES.PARTITION_ID.null.10",
                "SYS.PARTITION_STATES.NODE_ID.null.2147483647",
                "SYS.PARTITION_STATES.STATE.null.2147483647",
                "SYS.PARTITION_STATES.IS_PRIMARY.null.1",
                "SYS.BINARY_METADATA.FIELDS.null.2147483647",
                "SYS.BINARY_METADATA.AFF_KEY_FIELD_NAME.null.2147483647",
                "SYS.BINARY_METADATA.SCHEMAS_IDS.null.2147483647",
                "SYS.BINARY_METADATA.TYPE_ID.null.10",
                "SYS.BINARY_METADATA.IS_ENUM.null.1",
                "SYS.BINARY_METADATA.FIELDS_COUNT.null.10",
                "SYS.BINARY_METADATA.TYPE_NAME.null.2147483647",
                "SYS.DISTRIBUTED_METASTORAGE.NAME.null.2147483647",
                "SYS.DISTRIBUTED_METASTORAGE.VALUE.null.2147483647"
                ));

            Assert.assertEquals(expectedCols, actualSystemCols);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
    @Test
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

            while (rs.next()) {
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
    @Test
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
    @Test
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
                "PUBLIC.TEST_DECIMAL_COLUMN.ID.ID",
                "PUBLIC.TEST_DECIMAL_COLUMN_PRECISION.ID.ID",
                "PUBLIC.TEST_DECIMAL_DATE_COLUMN_META.ID.ID"));

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
            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement noParams = conn.prepareStatement("select * from Person;");
                ParameterMetaData params = noParams.getParameterMetaData();

                assertEquals("Parameters should be empty.", 0, params.getParameterCount());
            }

            // Selects.
            try (Connection conn = DriverManager.getConnection(URL)) {
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
            try (Connection conn = DriverManager.getConnection(URL)) {
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

            // Multistatement
            try (Connection conn = DriverManager.getConnection(URL)) {
                conn.setSchema("\"pers\"");

                PreparedStatement updateStmt = conn.prepareStatement(
                    "update Person p set orgId = 42 where p.name > ? and p.orgId > ?;" +
                        "select orgId from Person p where p.name > ? and p.orgId > ?");

                ParameterMetaData meta = updateStmt.getParameterMetaData();

                assertNotNull(meta);

                assertEquals(4, meta.getParameterCount());

                assertEquals(Types.VARCHAR, meta.getParameterType(1));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(1));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(1));

                assertEquals(Types.INTEGER, meta.getParameterType(2));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(2));

                assertEquals(Types.VARCHAR, meta.getParameterType(3));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(3));
                assertEquals(Integer.MAX_VALUE, meta.getPrecision(3));

                assertEquals(Types.INTEGER, meta.getParameterType(4));
                assertEquals(ParameterMetaData.parameterNullableUnknown, meta.isNullable(4));
            }
        }
    }

    /**
     * Check that parameters metadata throws correct exception on non-parsable statement.
     */
    @Test
    public void testParametersMetadataNegative() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
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
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getSchemas();

            Set<String> expectedSchemas = new HashSet<>(Arrays.asList(SCHEMA_SYS, DFLT_SCHEMA,
                "pers", "org", "dep", "PREDEFINED_SCHEMAS_1", "PREDEFINED_SCHEMAS_2"));

            Set<String> schemas = new HashSet<>();

            while (rs.next()) {
                schemas.add(rs.getString(1));

                assertEquals("There is only one possible catalog.",
                    JdbcUtils.CATALOG_NAME, rs.getString(2));
            }

            assert expectedSchemas.equals(schemas) : "Unexpected schemas: " + schemas +
                ". Expected schemas: " + expectedSchemas;
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
        try (Connection conn = DriverManager.getConnection(URL)) {
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
    public void testEmptySchemasMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getSchemas(null, "qqq");

            assert !rs.next() : "Empty result set is expected";
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersions() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            assertEquals("Unexpected ignite database product version.",
                conn.getMetaData().getDatabaseProductVersion(), IgniteVersionUtils.VER.toString());
            assertEquals("Unexpected ignite driver version.",
                conn.getMetaData().getDriverVersion(), IgniteVersionUtils.VER.toString());
        }

        try (Connection conn = DriverManager.getConnection(URL_PARTITION_AWARENESS)) {
            assertEquals("Unexpected ignite database product version.",
                conn.getMetaData().getDatabaseProductVersion(), IgniteVersionUtils.VER.toString());
            assertEquals("Unexpected ignite driver version.",
                conn.getMetaData().getDriverVersion(), IgniteVersionUtils.VER.toString());
        }
    }

    /**
     * Check JDBC support flags.
     */
    @Test
    public void testCheckSupports() throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL)) {
            DatabaseMetaData meta = conn.getMetaData();

            assertTrue(meta.supportsANSI92EntryLevelSQL());
            assertTrue(meta.supportsAlterTableWithAddColumn());
            assertTrue(meta.supportsAlterTableWithDropColumn());
            assertTrue(meta.nullPlusNonNullIsNull());
        }
    }

    /**
     * Person.
     */
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

    /** */
    private enum MetadataColumn {
        /** */
        TABLE_SCHEMA("TABLE_SCHEM"),

        /** */
        COLUMN_NAME("COLUMN_NAME");

        /** Column name. */
        private final String colName;

        /**
         * @param colName Column name.
         */
        MetadataColumn(String colName) {
            this.colName = colName;
        }

        /** */
        public String columnName() {
            return colName;
        }
    }
}
