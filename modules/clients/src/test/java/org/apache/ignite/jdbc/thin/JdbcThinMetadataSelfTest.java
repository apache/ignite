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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.systemview.view.sql.SqlIndexView;
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
import static org.apache.ignite.internal.jdbc2.JdbcUtils.CATALOG_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.SCHEMA_SYS;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.schema.management.SchemaManager.SQL_IDXS_VIEW;
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
                .setSqlSchemas("PREDEFINED_SCHEMAS_1", "PREDEFINED_SCHEMAS_2"));
    }

    /**
     * @param qryEntity Query entity.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(QueryEntity qryEntity) {
        CacheConfiguration<?, ?> cache = defaultCacheConfiguration();

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
            assertEquals(CATALOG_NAME, rs.getString("TABLE_CAT"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            assertFalse(rs.next());

            rs = meta.getTables(null, "org", "%", new String[]{"TABLE"});
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals(CATALOG_NAME, rs.getString("TABLE_CAT"));
            assertEquals("ORGANIZATION", rs.getString("TABLE_NAME"));

            assertFalse(rs.next());

            rs = meta.getTables(null, "pers", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals(CATALOG_NAME, rs.getString("TABLE_CAT"));
            assertEquals("PERSON", rs.getString("TABLE_NAME"));

            assertFalse(rs.next());

            rs = meta.getTables(null, "org", "%", null);
            assertNotNull(rs);
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            assertEquals(CATALOG_NAME, rs.getString("TABLE_CAT"));
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
            new TreeSet<>(Arrays.asList(
                "SYS.METRICS",
                "SYS.SERVICES",
                "SYS.CACHE_GROUPS",
                "SYS.CACHES",
                "SYS.TASKS",
                "SYS.JOBS",
                "SYS.SQL_QUERIES_HISTORY",
                "SYS.NODES",
                "SYS.CONFIGURATION",
                "SYS.SCHEMAS",
                "SYS.NODE_METRICS",
                "SYS.BASELINE_NODES",
                "SYS.BASELINE_NODE_ATTRIBUTES",
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
                "SYS.DISTRIBUTED_METASTORAGE",
                "SYS.DS_QUEUES",
                "SYS.DS_SETS",
                "SYS.DS_ATOMICSEQUENCES",
                "SYS.DS_ATOMICLONGS",
                "SYS.DS_ATOMICREFERENCES",
                "SYS.DS_ATOMICSTAMPED",
                "SYS.DS_COUNTDOWNLATCHES",
                "SYS.DS_SEMAPHORES",
                "SYS.DS_REENTRANTLOCKS",
                "SYS.STATISTICS_LOCAL_DATA",
                "SYS.STATISTICS_GLOBAL_DATA",
                "SYS.STATISTICS_PARTITION_DATA",
                "SYS.STATISTICS_CONFIGURATION",
                "SYS.PAGES_TIMESTAMP_HISTOGRAM"
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

            Set<String> actualTbls = new TreeSet<>();

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
                }
                else if ("ORGID".equals(name)) {
                    assert rs.getInt("DATA_TYPE") == INTEGER;
                    assert "INTEGER".equals(rs.getString("TYPE_NAME"));
                    assert rs.getInt("NULLABLE") == 1;
                    assert rs.getInt(11) == 1;  // nullable column by index
                    assert rs.getString("IS_NULLABLE").equals("YES");
                }
                else if ("AGE".equals(name)) {
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
                }
                else if ("name".equals(name)) {
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

            Set<String> expectedCols = new TreeSet<>(Arrays.asList(
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

            Set<String> actualUserCols = new TreeSet<>();

            Set<String> actualSystemCols = new TreeSet<>();

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

            expectedCols = new TreeSet<>(Arrays.asList(
                "SYS.BASELINE_NODES.CONSISTENT_ID.null",
                "SYS.BASELINE_NODES.ONLINE.null",
                "SYS.BASELINE_NODE_ATTRIBUTES.NODE_CONSISTENT_ID.null",
                "SYS.BASELINE_NODE_ATTRIBUTES.NAME.null",
                "SYS.BASELINE_NODE_ATTRIBUTES.VALUE.null",
                "SYS.CACHES.CACHE_GROUP_ID.null",
                "SYS.CACHES.CACHE_GROUP_NAME.null",
                "SYS.CACHES.CACHE_ID.null",
                "SYS.CACHES.CACHE_NAME.null",
                "SYS.CACHES.CACHE_TYPE.null",
                "SYS.CACHES.CACHE_MODE.null",
                "SYS.CACHES.ATOMICITY_MODE.null",
                "SYS.CACHES.IS_ONHEAP_CACHE_ENABLED.null",
                "SYS.CACHES.IS_COPY_ON_READ.null",
                "SYS.CACHES.IS_LOAD_PREVIOUS_VALUE.null",
                "SYS.CACHES.IS_READ_FROM_BACKUP.null",
                "SYS.CACHES.PARTITION_LOSS_POLICY.null",
                "SYS.CACHES.NODE_FILTER.null",
                "SYS.CACHES.TOPOLOGY_VALIDATOR.null",
                "SYS.CACHES.IS_EAGER_TTL.null",
                "SYS.CACHES.WRITE_SYNCHRONIZATION_MODE.null",
                "SYS.CACHES.IS_INVALIDATE.null",
                "SYS.CACHES.IS_EVENTS_DISABLED.null",
                "SYS.CACHES.IS_STATISTICS_ENABLED.null",
                "SYS.CACHES.IS_MANAGEMENT_ENABLED.null",
                "SYS.CACHES.BACKUPS.null",
                "SYS.CACHES.AFFINITY.null",
                "SYS.CACHES.AFFINITY_MAPPER.null",
                "SYS.CACHES.REBALANCE_MODE.null",
                "SYS.CACHES.REBALANCE_BATCH_SIZE.null",
                "SYS.CACHES.REBALANCE_TIMEOUT.null",
                "SYS.CACHES.REBALANCE_DELAY.null",
                "SYS.CACHES.REBALANCE_THROTTLE.null",
                "SYS.CACHES.REBALANCE_BATCHES_PREFETCH_COUNT.null",
                "SYS.CACHES.REBALANCE_ORDER.null",
                "SYS.CACHES.EVICTION_FILTER.null",
                "SYS.CACHES.EVICTION_POLICY_FACTORY.null",
                "SYS.CACHES.CONFLICT_RESOLVER.null",
                "SYS.CACHES.IS_NEAR_CACHE_ENABLED.null",
                "SYS.CACHES.NEAR_CACHE_EVICTION_POLICY_FACTORY.null",
                "SYS.CACHES.NEAR_CACHE_START_SIZE.null",
                "SYS.CACHES.DEFAULT_LOCK_TIMEOUT.null",
                "SYS.CACHES.INTERCEPTOR.null",
                "SYS.CACHES.CACHE_STORE_FACTORY.null",
                "SYS.CACHES.IS_STORE_KEEP_BINARY.null",
                "SYS.CACHES.IS_READ_THROUGH.null",
                "SYS.CACHES.IS_WRITE_THROUGH.null",
                "SYS.CACHES.IS_WRITE_BEHIND_ENABLED.null",
                "SYS.CACHES.WRITE_BEHIND_COALESCING.null",
                "SYS.CACHES.WRITE_BEHIND_FLUSH_SIZE.null",
                "SYS.CACHES.WRITE_BEHIND_FLUSH_FREQUENCY.null",
                "SYS.CACHES.WRITE_BEHIND_FLUSH_THREAD_COUNT.null",
                "SYS.CACHES.WRITE_BEHIND_BATCH_SIZE.null",
                "SYS.CACHES.MAX_CONCURRENT_ASYNC_OPERATIONS.null",
                "SYS.CACHES.CACHE_LOADER_FACTORY.null",
                "SYS.CACHES.CACHE_WRITER_FACTORY.null",
                "SYS.CACHES.EXPIRY_POLICY_FACTORY.null",
                "SYS.CACHES.IS_SQL_ESCAPE_ALL.null",
                "SYS.CACHES.IS_ENCRYPTION_ENABLED.null",
                "SYS.CACHES.SQL_SCHEMA.null",
                "SYS.CACHES.SQL_INDEX_MAX_INLINE_SIZE.null",
                "SYS.CACHES.IS_SQL_ONHEAP_CACHE_ENABLED.null",
                "SYS.CACHES.SQL_ONHEAP_CACHE_MAX_SIZE.null",
                "SYS.CACHES.QUERY_DETAIL_METRICS_SIZE.null",
                "SYS.CACHES.QUERY_PARALLELISM.null",
                "SYS.CACHES.MAX_QUERY_ITERATORS_COUNT.null",
                "SYS.CACHES.DATA_REGION_NAME.null",
                "SYS.CACHE_GROUPS.CACHE_GROUP_ID.null",
                "SYS.CACHE_GROUPS.CACHE_GROUP_NAME.null",
                "SYS.CACHE_GROUPS.IS_SHARED.null",
                "SYS.CACHE_GROUPS.CACHE_COUNT.null",
                "SYS.CACHE_GROUPS.CACHE_MODE.null",
                "SYS.CACHE_GROUPS.ATOMICITY_MODE.null",
                "SYS.CACHE_GROUPS.AFFINITY.null",
                "SYS.CACHE_GROUPS.PARTITIONS_COUNT.null",
                "SYS.CACHE_GROUPS.NODE_FILTER.null",
                "SYS.CACHE_GROUPS.DATA_REGION_NAME.null",
                "SYS.CACHE_GROUPS.TOPOLOGY_VALIDATOR.null",
                "SYS.CACHE_GROUPS.PARTITION_LOSS_POLICY.null",
                "SYS.CACHE_GROUPS.REBALANCE_MODE.null",
                "SYS.CACHE_GROUPS.REBALANCE_DELAY.null",
                "SYS.CACHE_GROUPS.REBALANCE_ORDER.null",
                "SYS.CACHE_GROUPS.BACKUPS.null",
                "SYS.INDEXES.CACHE_GROUP_ID.null",
                "SYS.INDEXES.CACHE_GROUP_NAME.null",
                "SYS.INDEXES.CACHE_ID.null",
                "SYS.INDEXES.CACHE_NAME.null",
                "SYS.INDEXES.SCHEMA_NAME.null",
                "SYS.INDEXES.TABLE_NAME.null",
                "SYS.INDEXES.INDEX_NAME.null",
                "SYS.INDEXES.INDEX_TYPE.null",
                "SYS.INDEXES.COLUMNS.null",
                "SYS.INDEXES.IS_PK.null",
                "SYS.INDEXES.IS_UNIQUE.null",
                "SYS.INDEXES.INLINE_SIZE.null",
                "SYS.LOCAL_CACHE_GROUPS_IO.CACHE_GROUP_ID.null",
                "SYS.LOCAL_CACHE_GROUPS_IO.CACHE_GROUP_NAME.null",
                "SYS.LOCAL_CACHE_GROUPS_IO.PHYSICAL_READS.null",
                "SYS.LOCAL_CACHE_GROUPS_IO.LOGICAL_READS.null",
                "SYS.SQL_QUERIES_HISTORY.SCHEMA_NAME.null",
                "SYS.SQL_QUERIES_HISTORY.SQL.null",
                "SYS.SQL_QUERIES_HISTORY.LOCAL.null",
                "SYS.SQL_QUERIES_HISTORY.EXECUTIONS.null",
                "SYS.SQL_QUERIES_HISTORY.FAILURES.null",
                "SYS.SQL_QUERIES_HISTORY.DURATION_MIN.null",
                "SYS.SQL_QUERIES_HISTORY.DURATION_MAX.null",
                "SYS.SQL_QUERIES_HISTORY.LAST_START_TIME.null",
                "SYS.SQL_QUERIES.QUERY_ID.null",
                "SYS.SQL_QUERIES.SQL.null",
                "SYS.SQL_QUERIES.SCHEMA_NAME.null",
                "SYS.SQL_QUERIES.LOCAL.null",
                "SYS.SQL_QUERIES.START_TIME.null",
                "SYS.SQL_QUERIES.DURATION.null",
                "SYS.SQL_QUERIES.ORIGIN_NODE_ID.null",
                "SYS.SQL_QUERIES.INITIATOR_ID.null",
                "SYS.SQL_QUERIES.SUBJECT_ID.null",
                "SYS.SCAN_QUERIES.START_TIME.null",
                "SYS.SCAN_QUERIES.TRANSFORMER.null",
                "SYS.SCAN_QUERIES.LOCAL.null",
                "SYS.SCAN_QUERIES.QUERY_ID.null",
                "SYS.SCAN_QUERIES.PARTITION.null",
                "SYS.SCAN_QUERIES.CACHE_GROUP_ID.null",
                "SYS.SCAN_QUERIES.CACHE_NAME.null",
                "SYS.SCAN_QUERIES.TOPOLOGY.null",
                "SYS.SCAN_QUERIES.CACHE_GROUP_NAME.null",
                "SYS.SCAN_QUERIES.TASK_NAME.null",
                "SYS.SCAN_QUERIES.DURATION.null",
                "SYS.SCAN_QUERIES.KEEP_BINARY.null",
                "SYS.SCAN_QUERIES.FILTER.null",
                "SYS.SCAN_QUERIES.SUBJECT_ID.null",
                "SYS.SCAN_QUERIES.CANCELED.null",
                "SYS.SCAN_QUERIES.CACHE_ID.null",
                "SYS.SCAN_QUERIES.PAGE_SIZE.null",
                "SYS.SCAN_QUERIES.ORIGIN_NODE_ID.null",
                "SYS.NODES.NODE_ID.null",
                "SYS.NODES.CONSISTENT_ID.null",
                "SYS.NODES.VERSION.null",
                "SYS.NODES.IS_CLIENT.null",
                "SYS.NODES.IS_LOCAL.null",
                "SYS.NODES.NODE_ORDER.null",
                "SYS.NODES.ADDRESSES.null",
                "SYS.NODES.HOSTNAMES.null",
                "SYS.NODE_ATTRIBUTES.NODE_ID.null",
                "SYS.NODE_ATTRIBUTES.NAME.null",
                "SYS.NODE_ATTRIBUTES.VALUE.null",
                "SYS.CONFIGURATION.NAME.null",
                "SYS.CONFIGURATION.VALUE.null",
                "SYS.NODE_METRICS.NODE_ID.null",
                "SYS.NODE_METRICS.LAST_UPDATE_TIME.null",
                "SYS.NODE_METRICS.MAX_ACTIVE_JOBS.null",
                "SYS.NODE_METRICS.CUR_ACTIVE_JOBS.null",
                "SYS.NODE_METRICS.AVG_ACTIVE_JOBS.null",
                "SYS.NODE_METRICS.MAX_WAITING_JOBS.null",
                "SYS.NODE_METRICS.CUR_WAITING_JOBS.null",
                "SYS.NODE_METRICS.AVG_WAITING_JOBS.null",
                "SYS.NODE_METRICS.MAX_REJECTED_JOBS.null",
                "SYS.NODE_METRICS.CUR_REJECTED_JOBS.null",
                "SYS.NODE_METRICS.AVG_REJECTED_JOBS.null",
                "SYS.NODE_METRICS.TOTAL_REJECTED_JOBS.null",
                "SYS.NODE_METRICS.MAX_CANCELED_JOBS.null",
                "SYS.NODE_METRICS.CUR_CANCELED_JOBS.null",
                "SYS.NODE_METRICS.AVG_CANCELED_JOBS.null",
                "SYS.NODE_METRICS.TOTAL_CANCELED_JOBS.null",
                "SYS.NODE_METRICS.MAX_JOBS_WAIT_TIME.null",
                "SYS.NODE_METRICS.CUR_JOBS_WAIT_TIME.null",
                "SYS.NODE_METRICS.AVG_JOBS_WAIT_TIME.null",
                "SYS.NODE_METRICS.MAX_JOBS_EXECUTE_TIME.null",
                "SYS.NODE_METRICS.CUR_JOBS_EXECUTE_TIME.null",
                "SYS.NODE_METRICS.AVG_JOBS_EXECUTE_TIME.null",
                "SYS.NODE_METRICS.TOTAL_JOBS_EXECUTE_TIME.null",
                "SYS.NODE_METRICS.TOTAL_EXECUTED_JOBS.null",
                "SYS.NODE_METRICS.TOTAL_EXECUTED_TASKS.null",
                "SYS.NODE_METRICS.TOTAL_BUSY_TIME.null",
                "SYS.NODE_METRICS.TOTAL_IDLE_TIME.null",
                "SYS.NODE_METRICS.CUR_IDLE_TIME.null",
                "SYS.NODE_METRICS.BUSY_TIME_PERCENTAGE.null",
                "SYS.NODE_METRICS.IDLE_TIME_PERCENTAGE.null",
                "SYS.NODE_METRICS.TOTAL_CPU.null",
                "SYS.NODE_METRICS.CUR_CPU_LOAD.null",
                "SYS.NODE_METRICS.AVG_CPU_LOAD.null",
                "SYS.NODE_METRICS.CUR_GC_CPU_LOAD.null",
                "SYS.NODE_METRICS.HEAP_MEMORY_INIT.null",
                "SYS.NODE_METRICS.HEAP_MEMORY_USED.null",
                "SYS.NODE_METRICS.HEAP_MEMORY_COMMITED.null",
                "SYS.NODE_METRICS.HEAP_MEMORY_MAX.null",
                "SYS.NODE_METRICS.HEAP_MEMORY_TOTAL.null",
                "SYS.NODE_METRICS.NONHEAP_MEMORY_INIT.null",
                "SYS.NODE_METRICS.NONHEAP_MEMORY_USED.null",
                "SYS.NODE_METRICS.NONHEAP_MEMORY_COMMITED.null",
                "SYS.NODE_METRICS.NONHEAP_MEMORY_MAX.null",
                "SYS.NODE_METRICS.NONHEAP_MEMORY_TOTAL.null",
                "SYS.NODE_METRICS.UPTIME.null",
                "SYS.NODE_METRICS.JVM_START_TIME.null",
                "SYS.NODE_METRICS.NODE_START_TIME.null",
                "SYS.NODE_METRICS.LAST_DATA_VERSION.null",
                "SYS.NODE_METRICS.CUR_THREAD_COUNT.null",
                "SYS.NODE_METRICS.MAX_THREAD_COUNT.null",
                "SYS.NODE_METRICS.TOTAL_THREAD_COUNT.null",
                "SYS.NODE_METRICS.CUR_DAEMON_THREAD_COUNT.null",
                "SYS.NODE_METRICS.SENT_MESSAGES_COUNT.null",
                "SYS.NODE_METRICS.SENT_BYTES_COUNT.null",
                "SYS.NODE_METRICS.RECEIVED_MESSAGES_COUNT.null",
                "SYS.NODE_METRICS.RECEIVED_BYTES_COUNT.null",
                "SYS.NODE_METRICS.OUTBOUND_MESSAGES_QUEUE.null",
                "SYS.TABLES.CACHE_GROUP_ID.null",
                "SYS.TABLES.CACHE_GROUP_NAME.null",
                "SYS.TABLES.CACHE_ID.null",
                "SYS.TABLES.CACHE_NAME.null",
                "SYS.TABLES.SCHEMA_NAME.null",
                "SYS.TABLES.TABLE_NAME.null",
                "SYS.TABLES.AFFINITY_KEY_COLUMN.null",
                "SYS.TABLES.KEY_ALIAS.null",
                "SYS.TABLES.VALUE_ALIAS.null",
                "SYS.TABLES.KEY_TYPE_NAME.null",
                "SYS.TABLES.VALUE_TYPE_NAME.null",
                "SYS.TABLES.IS_INDEX_REBUILD_IN_PROGRESS.null",
                "SYS.METRICS.NAME.null",
                "SYS.METRICS.VALUE.null",
                "SYS.METRICS.DESCRIPTION.null",
                "SYS.SERVICES.SERVICE_ID.null",
                "SYS.SERVICES.NAME.null",
                "SYS.SERVICES.SERVICE_CLASS.null",
                "SYS.SERVICES.CACHE_NAME.null",
                "SYS.SERVICES.ORIGIN_NODE_ID.null",
                "SYS.SERVICES.TOTAL_COUNT.null",
                "SYS.SERVICES.MAX_PER_NODE_COUNT.null",
                "SYS.SERVICES.AFFINITY_KEY.null",
                "SYS.SERVICES.NODE_FILTER.null",
                "SYS.SERVICES.STATICALLY_CONFIGURED.null",
                "SYS.SERVICES.SERVICE_ID.null",
                "SYS.TASKS.AFFINITY_CACHE_NAME.null",
                "SYS.TASKS.INTERNAL.null",
                "SYS.TASKS.END_TIME.null",
                "SYS.TASKS.START_TIME.null",
                "SYS.TASKS.USER_VERSION.null",
                "SYS.TASKS.TASK_NAME.null",
                "SYS.TASKS.TASK_NODE_ID.null",
                "SYS.TASKS.JOB_ID.null",
                "SYS.TASKS.ID.null",
                "SYS.TASKS.SESSION_ID.null",
                "SYS.TASKS.AFFINITY_PARTITION_ID.null",
                "SYS.TASKS.TASK_CLASS_NAME.null",
                "SYS.JOBS.IS_STARTED.null",
                "SYS.JOBS.EXECUTOR_NAME.null",
                "SYS.JOBS.IS_TIMED_OUT.null",
                "SYS.JOBS.ID.null",
                "SYS.JOBS.FINISH_TIME.null",
                "SYS.JOBS.IS_INTERNAL.null",
                "SYS.JOBS.CREATE_TIME.null",
                "SYS.JOBS.AFFINITY_PARTITION_ID.null",
                "SYS.JOBS.ORIGIN_NODE_ID.null",
                "SYS.JOBS.TASK_NAME.null",
                "SYS.JOBS.TASK_CLASS_NAME.null",
                "SYS.JOBS.SESSION_ID.null",
                "SYS.JOBS.IS_FINISHING.null",
                "SYS.JOBS.START_TIME.null",
                "SYS.JOBS.AFFINITY_CACHE_IDS.null",
                "SYS.JOBS.STATE.null",
                "SYS.CLIENT_CONNECTIONS.CONNECTION_ID.null",
                "SYS.CLIENT_CONNECTIONS.LOCAL_ADDRESS.null",
                "SYS.CLIENT_CONNECTIONS.REMOTE_ADDRESS.null",
                "SYS.CLIENT_CONNECTIONS.TYPE.null",
                "SYS.CLIENT_CONNECTIONS.USER.null",
                "SYS.CLIENT_CONNECTIONS.VERSION.null",
                "SYS.TASKS.EXEC_NAME.null",
                "SYS.TRANSACTIONS.LOCAL_NODE_ID.null",
                "SYS.TRANSACTIONS.STATE.null",
                "SYS.TRANSACTIONS.XID.null",
                "SYS.TRANSACTIONS.LABEL.null",
                "SYS.TRANSACTIONS.START_TIME.null",
                "SYS.TRANSACTIONS.ISOLATION.null",
                "SYS.TRANSACTIONS.CONCURRENCY.null",
                "SYS.TRANSACTIONS.COLOCATED.null",
                "SYS.TRANSACTIONS.DHT.null",
                "SYS.TRANSACTIONS.IMPLICIT.null",
                "SYS.TRANSACTIONS.IMPLICIT_SINGLE.null",
                "SYS.TRANSACTIONS.INTERNAL.null",
                "SYS.TRANSACTIONS.LOCAL.null",
                "SYS.TRANSACTIONS.NEAR.null",
                "SYS.TRANSACTIONS.ONE_PHASE_COMMIT.null",
                "SYS.TRANSACTIONS.SUBJECT_ID.null",
                "SYS.TRANSACTIONS.SYSTEM.null",
                "SYS.TRANSACTIONS.THREAD_ID.null",
                "SYS.TRANSACTIONS.TIMEOUT.null",
                "SYS.TRANSACTIONS.DURATION.null",
                "SYS.TRANSACTIONS.ORIGINATING_NODE_ID.null",
                "SYS.TRANSACTIONS.OTHER_NODE_ID.null",
                "SYS.TRANSACTIONS.TOP_VER.null",
                "SYS.TRANSACTIONS.KEYS_COUNT.null",
                "SYS.TRANSACTIONS.CACHE_IDS.null",
                "SYS.SCHEMAS.SCHEMA_NAME.null",
                "SYS.SCHEMAS.PREDEFINED.null",
                "SYS.VIEWS.NAME.null",
                "SYS.VIEWS.DESCRIPTION.null",
                "SYS.VIEWS.SCHEMA.null",
                "SYS.TABLE_COLUMNS.AFFINITY_COLUMN.null",
                "SYS.TABLE_COLUMNS.COLUMN_NAME.null",
                "SYS.TABLE_COLUMNS.SCALE.null",
                "SYS.TABLE_COLUMNS.PK.null",
                "SYS.TABLE_COLUMNS.TYPE.null",
                "SYS.TABLE_COLUMNS.DEFAULT_VALUE.null",
                "SYS.TABLE_COLUMNS.SCHEMA_NAME.null",
                "SYS.TABLE_COLUMNS.TABLE_NAME.null",
                "SYS.TABLE_COLUMNS.NULLABLE.null",
                "SYS.TABLE_COLUMNS.PRECISION.null",
                "SYS.TABLE_COLUMNS.AUTO_INCREMENT.null",
                "SYS.VIEW_COLUMNS.NULLABLE.null",
                "SYS.VIEW_COLUMNS.SCHEMA_NAME.null",
                "SYS.VIEW_COLUMNS.COLUMN_NAME.null",
                "SYS.VIEW_COLUMNS.TYPE.null",
                "SYS.VIEW_COLUMNS.PRECISION.null",
                "SYS.VIEW_COLUMNS.DEFAULT_VALUE.null",
                "SYS.VIEW_COLUMNS.SCALE.null",
                "SYS.VIEW_COLUMNS.VIEW_NAME.null",
                "SYS.CONTINUOUS_QUERIES.NOTIFY_EXISTING.null",
                "SYS.CONTINUOUS_QUERIES.OLD_VALUE_REQUIRED.null",
                "SYS.CONTINUOUS_QUERIES.KEEP_BINARY.null",
                "SYS.CONTINUOUS_QUERIES.IS_MESSAGING.null",
                "SYS.CONTINUOUS_QUERIES.AUTO_UNSUBSCRIBE.null",
                "SYS.CONTINUOUS_QUERIES.LAST_SEND_TIME.null",
                "SYS.CONTINUOUS_QUERIES.LOCAL_TRANSFORMED_LISTENER.null",
                "SYS.CONTINUOUS_QUERIES.TOPIC.null",
                "SYS.CONTINUOUS_QUERIES.BUFFER_SIZE.null",
                "SYS.CONTINUOUS_QUERIES.REMOTE_TRANSFORMER.null",
                "SYS.CONTINUOUS_QUERIES.DELAYED_REGISTER.null",
                "SYS.CONTINUOUS_QUERIES.IS_QUERY.null",
                "SYS.CONTINUOUS_QUERIES.NODE_ID.null",
                "SYS.CONTINUOUS_QUERIES.INTERVAL.null",
                "SYS.CONTINUOUS_QUERIES.IS_EVENTS.null",
                "SYS.CONTINUOUS_QUERIES.ROUTINE_ID.null",
                "SYS.CONTINUOUS_QUERIES.REMOTE_FILTER.null",
                "SYS.CONTINUOUS_QUERIES.CACHE_NAME.null",
                "SYS.CONTINUOUS_QUERIES.LOCAL_LISTENER.null",
                "SYS.STRIPED_THREADPOOL_QUEUE.STRIPE_INDEX.null",
                "SYS.STRIPED_THREADPOOL_QUEUE.DESCRIPTION.null",
                "SYS.STRIPED_THREADPOOL_QUEUE.THREAD_NAME.null",
                "SYS.STRIPED_THREADPOOL_QUEUE.TASK_NAME.null",
                "SYS.DATASTREAM_THREADPOOL_QUEUE.STRIPE_INDEX.null",
                "SYS.DATASTREAM_THREADPOOL_QUEUE.DESCRIPTION.null",
                "SYS.DATASTREAM_THREADPOOL_QUEUE.THREAD_NAME.null",
                "SYS.DATASTREAM_THREADPOOL_QUEUE.TASK_NAME.null",
                "SYS.CACHE_GROUP_PAGE_LISTS.CACHE_GROUP_ID.null",
                "SYS.CACHE_GROUP_PAGE_LISTS.PARTITION_ID.null",
                "SYS.CACHE_GROUP_PAGE_LISTS.NAME.null",
                "SYS.CACHE_GROUP_PAGE_LISTS.BUCKET_NUMBER.null",
                "SYS.CACHE_GROUP_PAGE_LISTS.BUCKET_SIZE.null",
                "SYS.CACHE_GROUP_PAGE_LISTS.STRIPES_COUNT.null",
                "SYS.CACHE_GROUP_PAGE_LISTS.CACHED_PAGES_COUNT.null",
                "SYS.CACHE_GROUP_PAGE_LISTS.PAGE_FREE_SPACE.null",
                "SYS.DATA_REGION_PAGE_LISTS.NAME.null",
                "SYS.DATA_REGION_PAGE_LISTS.BUCKET_NUMBER.null",
                "SYS.DATA_REGION_PAGE_LISTS.BUCKET_SIZE.null",
                "SYS.DATA_REGION_PAGE_LISTS.STRIPES_COUNT.null",
                "SYS.DATA_REGION_PAGE_LISTS.CACHED_PAGES_COUNT.null",
                "SYS.DATA_REGION_PAGE_LISTS.PAGE_FREE_SPACE.null",
                "SYS.PARTITION_STATES.CACHE_GROUP_ID.null",
                "SYS.PARTITION_STATES.PARTITION_ID.null",
                "SYS.PARTITION_STATES.NODE_ID.null",
                "SYS.PARTITION_STATES.STATE.null",
                "SYS.PARTITION_STATES.IS_PRIMARY.null",
                "SYS.BINARY_METADATA.FIELDS.null",
                "SYS.BINARY_METADATA.AFF_KEY_FIELD_NAME.null",
                "SYS.BINARY_METADATA.SCHEMAS_IDS.null",
                "SYS.BINARY_METADATA.TYPE_ID.null",
                "SYS.BINARY_METADATA.IS_ENUM.null",
                "SYS.BINARY_METADATA.FIELDS_COUNT.null",
                "SYS.BINARY_METADATA.TYPE_NAME.null",
                "SYS.DISTRIBUTED_METASTORAGE.NAME.null",
                "SYS.DISTRIBUTED_METASTORAGE.VALUE.null",
                "SYS.DS_ATOMICLONGS.GROUP_ID.null",
                "SYS.DS_ATOMICLONGS.GROUP_NAME.null",
                "SYS.DS_ATOMICLONGS.NAME.null",
                "SYS.DS_ATOMICLONGS.REMOVED.null",
                "SYS.DS_ATOMICLONGS.VALUE.null",
                "SYS.DS_ATOMICREFERENCES.GROUP_ID.null",
                "SYS.DS_ATOMICREFERENCES.GROUP_NAME.null",
                "SYS.DS_ATOMICREFERENCES.NAME.null",
                "SYS.DS_ATOMICREFERENCES.REMOVED.null",
                "SYS.DS_ATOMICREFERENCES.VALUE.null",
                "SYS.DS_ATOMICSEQUENCES.BATCH_SIZE.null",
                "SYS.DS_ATOMICSEQUENCES.GROUP_ID.null",
                "SYS.DS_ATOMICSEQUENCES.GROUP_NAME.null",
                "SYS.DS_ATOMICSEQUENCES.NAME.null",
                "SYS.DS_ATOMICSEQUENCES.REMOVED.null",
                "SYS.DS_ATOMICSEQUENCES.VALUE.null",
                "SYS.DS_ATOMICSTAMPED.GROUP_ID.null",
                "SYS.DS_ATOMICSTAMPED.GROUP_NAME.null",
                "SYS.DS_ATOMICSTAMPED.NAME.null",
                "SYS.DS_ATOMICSTAMPED.REMOVED.null",
                "SYS.DS_ATOMICSTAMPED.STAMP.null",
                "SYS.DS_ATOMICSTAMPED.VALUE.null",
                "SYS.DS_COUNTDOWNLATCHES.AUTO_DELETE.null",
                "SYS.DS_COUNTDOWNLATCHES.COUNT.null",
                "SYS.DS_COUNTDOWNLATCHES.GROUP_ID.null",
                "SYS.DS_COUNTDOWNLATCHES.GROUP_NAME.null",
                "SYS.DS_COUNTDOWNLATCHES.INITIAL_COUNT.null",
                "SYS.DS_COUNTDOWNLATCHES.NAME.null",
                "SYS.DS_COUNTDOWNLATCHES.REMOVED.null",
                "SYS.DS_QUEUES.BOUNDED.null",
                "SYS.DS_QUEUES.CAPACITY.null",
                "SYS.DS_QUEUES.SIZE.null",
                "SYS.DS_QUEUES.COLLOCATED.null",
                "SYS.DS_QUEUES.GROUP_ID.null",
                "SYS.DS_QUEUES.GROUP_NAME.null",
                "SYS.DS_QUEUES.ID.null",
                "SYS.DS_QUEUES.NAME.null",
                "SYS.DS_QUEUES.REMOVED.null",
                "SYS.DS_REENTRANTLOCKS.BROKEN.null",
                "SYS.DS_REENTRANTLOCKS.FAILOVER_SAFE.null",
                "SYS.DS_REENTRANTLOCKS.FAIR.null",
                "SYS.DS_REENTRANTLOCKS.GROUP_ID.null",
                "SYS.DS_REENTRANTLOCKS.GROUP_NAME.null",
                "SYS.DS_REENTRANTLOCKS.HAS_QUEUED_THREADS.null",
                "SYS.DS_REENTRANTLOCKS.LOCKED.null",
                "SYS.DS_REENTRANTLOCKS.NAME.null",
                "SYS.DS_REENTRANTLOCKS.REMOVED.null",
                "SYS.DS_SEMAPHORES.AVAILABLE_PERMITS.null",
                "SYS.DS_SEMAPHORES.BROKEN.null",
                "SYS.DS_SEMAPHORES.FAILOVER_SAFE.null",
                "SYS.DS_SEMAPHORES.GROUP_ID.null",
                "SYS.DS_SEMAPHORES.GROUP_NAME.null",
                "SYS.DS_SEMAPHORES.HAS_QUEUED_THREADS.null",
                "SYS.DS_SEMAPHORES.NAME.null",
                "SYS.DS_SEMAPHORES.QUEUE_LENGTH.null",
                "SYS.DS_SEMAPHORES.REMOVED.null",
                "SYS.DS_SETS.COLLOCATED.null",
                "SYS.DS_SETS.GROUP_ID.null",
                "SYS.DS_SETS.GROUP_NAME.null",
                "SYS.DS_SETS.ID.null",
                "SYS.DS_SETS.NAME.null",
                "SYS.DS_SETS.REMOVED.null",
                "SYS.DS_SETS.SIZE.null",
                "SYS.STATISTICS_LOCAL_DATA.LAST_UPDATE_TIME.null",
                "SYS.STATISTICS_LOCAL_DATA.NAME.null",
                "SYS.STATISTICS_LOCAL_DATA.TOTAL.null",
                "SYS.STATISTICS_PARTITION_DATA.VERSION.null",
                "SYS.STATISTICS_CONFIGURATION.TYPE.null",
                "SYS.STATISTICS_PARTITION_DATA.NAME.null",
                "SYS.STATISTICS_CONFIGURATION.COLUMN.null",
                "SYS.STATISTICS_LOCAL_DATA.ROWS_COUNT.null",
                "SYS.STATISTICS_PARTITION_DATA.TYPE.null",
                "SYS.STATISTICS_LOCAL_DATA.DISTINCT.null",
                "SYS.STATISTICS_LOCAL_DATA.SIZE.null",
                "SYS.STATISTICS_PARTITION_DATA.LAST_UPDATE_TIME.null",
                "SYS.STATISTICS_CONFIGURATION.MAX_PARTITION_OBSOLESCENCE_PERCENT.null",
                "SYS.STATISTICS_LOCAL_DATA.VERSION.null",
                "SYS.STATISTICS_LOCAL_DATA.COLUMN.null",
                "SYS.STATISTICS_CONFIGURATION.SCHEMA.null",
                "SYS.STATISTICS_PARTITION_DATA.TOTAL.null",
                "SYS.STATISTICS_PARTITION_DATA.PARTITION.null",
                "SYS.STATISTICS_PARTITION_DATA.SCHEMA.null",
                "SYS.STATISTICS_PARTITION_DATA.ROWS_COUNT.null",
                "SYS.STATISTICS_PARTITION_DATA.SIZE.null",
                "SYS.STATISTICS_PARTITION_DATA.UPDATE_COUNTER.null",
                "SYS.STATISTICS_CONFIGURATION.NAME.null",
                "SYS.STATISTICS_PARTITION_DATA.DISTINCT.null",
                "SYS.STATISTICS_LOCAL_DATA.NULLS.null",
                "SYS.STATISTICS_CONFIGURATION.VERSION.null",
                "SYS.STATISTICS_CONFIGURATION.MANUAL_SIZE.null",
                "SYS.STATISTICS_CONFIGURATION.MANUAL_DISTINCT.null",
                "SYS.STATISTICS_CONFIGURATION.MANUAL_NULLS.null",
                "SYS.STATISTICS_CONFIGURATION.MANUAL_TOTAL.null",
                "SYS.STATISTICS_LOCAL_DATA.TYPE.null",
                "SYS.STATISTICS_PARTITION_DATA.NULLS.null",
                "SYS.STATISTICS_PARTITION_DATA.COLUMN.null",
                "SYS.STATISTICS_LOCAL_DATA.SCHEMA.null",
                "SYS.STATISTICS_GLOBAL_DATA.SCHEMA.null",
                "SYS.STATISTICS_GLOBAL_DATA.TYPE.null",
                "SYS.STATISTICS_GLOBAL_DATA.NAME.null",
                "SYS.STATISTICS_GLOBAL_DATA.COLUMN.null",
                "SYS.STATISTICS_GLOBAL_DATA.ROWS_COUNT.null",
                "SYS.STATISTICS_GLOBAL_DATA.DISTINCT.null",
                "SYS.STATISTICS_GLOBAL_DATA.NULLS.null",
                "SYS.STATISTICS_GLOBAL_DATA.TOTAL.null",
                "SYS.STATISTICS_GLOBAL_DATA.SIZE.null",
                "SYS.STATISTICS_GLOBAL_DATA.VERSION.null",
                "SYS.STATISTICS_GLOBAL_DATA.LAST_UPDATE_TIME.null",
                "SYS.PAGES_TIMESTAMP_HISTOGRAM.DATA_REGION_NAME.null",
                "SYS.PAGES_TIMESTAMP_HISTOGRAM.INTERVAL_START.null",
                "SYS.PAGES_TIMESTAMP_HISTOGRAM.INTERVAL_END.null",
                "SYS.PAGES_TIMESTAMP_HISTOGRAM.PAGES_COUNT.null"
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
        List<String> expectedIdxs = Arrays.asList(
            "_key_PK.1._KEY.A",
            "PERSON_NAME_ASC_AGE_DESC_IDX.1.NAME.A",
            "PERSON_NAME_ASC_AGE_DESC_IDX.2.AGE.D",
            "PERSON_NAME_ASC_AGE_DESC_IDX.3._KEY.A",
            "PERSON_ORGID_ASC_IDX.1.ORGID.A",
            "PERSON_ORGID_ASC_IDX.2._KEY.A"
        );

        try (Connection conn = DriverManager.getConnection(URL);
             ResultSet rs = conn.getMetaData().getIndexInfo(null, "pers", "PERSON", false, false)) {

            List<String> actualIdxs = new ArrayList<>();

            while (rs.next()) {
                actualIdxs.add(String.join(".",
                    rs.getString("INDEX_NAME"),
                    String.valueOf(rs.getInt("ORDINAL_POSITION")),
                    rs.getString("COLUMN_NAME"),
                    rs.getString("ASC_OR_DESC")));

                // Below values are constant for a cache
                assertEquals(CATALOG_NAME, rs.getString("TABLE_CAT"));
                assertEquals("pers", rs.getString("TABLE_SCHEM"));
                assertEquals("PERSON", rs.getString("TABLE_NAME"));
                assertNull(rs.getObject("INDEX_QUALIFIER"));
                assertEquals(DatabaseMetaData.tableIndexOther, rs.getShort("TYPE"));
                assertEquals(0, rs.getInt("CARDINALITY"));
                assertEquals(0, rs.getInt("PAGES"));
                assertNull(rs.getString("FILTER_CONDITION"));
            }

            assertEquals("Unexpected indexes count", expectedIdxs.size(), actualIdxs.size());

            for (int i = 0; i < actualIdxs.size(); i++)
                assertEquals("Unexpected index", expectedIdxs.get(i), actualIdxs.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllIndexes() throws Exception {
        try (Connection conn = DriverManager.getConnection(URL)) {
            ResultSet rs = conn.getMetaData().getIndexInfo(null, null, null, false, false);

            List<String> expectedIdxs = Arrays.asList(
                "PUBLIC.Quoted._key_PK.Id",
                "PUBLIC.Quoted.AFFINITY_KEY.Id",
                "PUBLIC.Quoted.MyTestIndex quoted.Id",
                "PUBLIC.TEST._key_PK.ID",
                "PUBLIC.TEST._key_PK.NAME",
                "PUBLIC.TEST.IDX.ID",
                "PUBLIC.TEST.IDX.NAME",
                "PUBLIC.TEST.IDX._KEY",
                "PUBLIC.TEST_DECIMAL_COLUMN._key_PK._KEY",
                "PUBLIC.TEST_DECIMAL_COLUMN._key_PK_proxy.ID",
                "PUBLIC.TEST_DECIMAL_COLUMN_PRECISION._key_PK._KEY",
                "PUBLIC.TEST_DECIMAL_COLUMN_PRECISION._key_PK_proxy.ID",
                "PUBLIC.TEST_DECIMAL_DATE_COLUMN_META._key_PK._KEY",
                "PUBLIC.TEST_DECIMAL_DATE_COLUMN_META._key_PK_proxy.ID",
                "dep.DEPARTMENT._key_PK._KEY",
                "org.ORGANIZATION._key_PK._KEY",
                "org.ORGANIZATION.ORGANIZATION_ID_ASC_IDX.ID",
                "org.ORGANIZATION.ORGANIZATION_ID_ASC_IDX._KEY",
                "org.ORGANIZATION.ORG_NAME_INDEX.NAME",
                "org.ORGANIZATION.ORG_NAME_INDEX._KEY",
                "pers.PERSON._key_PK._KEY",
                "pers.PERSON.PERSON_NAME_ASC_AGE_DESC_IDX.NAME",
                "pers.PERSON.PERSON_NAME_ASC_AGE_DESC_IDX.AGE",
                "pers.PERSON.PERSON_NAME_ASC_AGE_DESC_IDX._KEY",
                "pers.PERSON.PERSON_ORGID_ASC_IDX.ORGID",
                "pers.PERSON.PERSON_ORGID_ASC_IDX._KEY"
            );

            List<String> actualIdxs = new ArrayList<>();

            while (rs.next()) {
                actualIdxs.add(String.join(".",
                    rs.getString("TABLE_SCHEM"),
                    rs.getString("TABLE_NAME"),
                    rs.getString("INDEX_NAME"),
                    rs.getString("COLUMN_NAME")));
            }

            assertEquals("Unexpected indexes count", expectedIdxs.size(), actualIdxs.size());

            for (int i = 0; i < actualIdxs.size(); i++)
                assertEquals("Unexpected index", expectedIdxs.get(i), actualIdxs.get(i));
        }
    }

    /**
     *
     */
    @Test
    public void testIndexMetadataMatchesSystemView() throws Exception {
        Map<String, String> indexesFromMeta = new HashMap<>();

        try (Connection connection = DriverManager.getConnection(URL);
             ResultSet idxMeta = connection.getMetaData()
                 .getIndexInfo(null, null, null, false, false)) {

            while (idxMeta.next()) {
                String idxName = String.join(".",
                    idxMeta.getString("TABLE_SCHEM"),
                    idxMeta.getString("TABLE_NAME"),
                    idxMeta.getString("INDEX_NAME"));

                String fieldInfo = '"' + idxMeta.getString("COLUMN_NAME") + "\" " +
                    ("A".equals(idxMeta.getString("ASC_OR_DESC")) ? "ASC" : "DESC");

                indexesFromMeta.compute(idxName, (k, v) -> v == null ? fieldInfo : v + ", " + fieldInfo);

                // Check sorting by ordinal position
                int fieldsCnt = indexesFromMeta.get(idxName).split(", ").length;
                int ordinalPos = idxMeta.getInt("ORDINAL_POSITION");
                assertEquals("Unexpected ordinal position", ordinalPos, fieldsCnt);
            }
        }

        Map<String, String> indexesFromSysView = new HashMap<>();

        for (Object o : grid(0).context().systemView().view(SQL_IDXS_VIEW)) {
            SqlIndexView idxView = (SqlIndexView)o;

            String idxName = String.join(".",
                idxView.schemaName(),
                idxView.tableName(),
                idxView.indexName());

            indexesFromSysView.put(idxName, idxView.columns());
        }

        assertEqualsMaps(indexesFromSysView, indexesFromMeta);
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
                    CATALOG_NAME, rs.getString(2));
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
