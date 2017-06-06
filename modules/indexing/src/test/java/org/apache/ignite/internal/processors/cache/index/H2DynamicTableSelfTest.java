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

package org.apache.ignite.internal.processors.cache.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;

import javax.cache.CacheException;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.ddl.DdlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests for CREATE/DROP TABLE.
 */
public class H2DynamicTableSelfTest extends AbstractSchemaSelfTest {
    /** Client node index. */
    private final static int CLIENT = 2;

    /** */
    private final static String INDEXED_CACHE_NAME = CACHE_NAME + "_idx";

    /** */
    private final static String INDEXED_CACHE_NAME_2 = INDEXED_CACHE_NAME + "_2";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);

        client().addCacheConfiguration(cacheConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        client().getOrCreateCache(cacheConfigurationForIndexing());
        client().getOrCreateCache(cacheConfigurationForIndexingInPublicSchema());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (client().cache("Person") != null)
            executeDdl("DROP TABLE IF EXISTS PUBLIC.\"Person\"");


            executeDdl("DROP TABLE IF EXISTS PUBLIC.\"City\"");

        super.afterTest();
    }

    /**
     * Test that {@code CREATE TABLE} actually creates new cache, H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTable() throws Exception {
        doTestCreateTable(CACHE_NAME, null);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code REPLICATED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTableReplicated() throws Exception {
        doTestCreateTable("REPLICATED", CacheMode.REPLICATED);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code PARTITIONED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTablePartitioned() throws Exception {
        doTestCreateTable("PARTITIONED", CacheMode.PARTITIONED);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code REPLICATED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTableReplicatedCaseInsensitive() throws Exception {
        doTestCreateTable("replicated", CacheMode.REPLICATED);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code PARTITIONED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTablePartitionedCaseInsensitive() throws Exception {
        doTestCreateTable("partitioned", CacheMode.PARTITIONED);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code PARTITIONED} cache,
     * H2 table and type descriptor on all nodes, when no cache template name is given.
     * @throws Exception if failed.
     */
    public void testCreateTableNoTemplate() throws Exception {
        doTestCreateTable(null, CacheMode.PARTITIONED);
    }

    /**
     * Test that {@code CREATE TABLE} with given template cache name actually creates new cache,
     * H2 table and type descriptor on all nodes, optionally with cache type check.
     * @param tplCacheName Template cache name.
     * @param mode Expected cache mode, or {@code null} if no check is needed.
     */
    private void doTestCreateTable(String tplCacheName, CacheMode mode) {
        executeDdl("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
            (F.isEmpty(tplCacheName) ? "" : "\"template=" + tplCacheName + "\",") + "\"backups=10,atomicity=atomic\"");

        for (int i = 0; i < 4; i++) {
            IgniteEx node = grid(i);

            assertNotNull(node.cache("Person"));

            DynamicCacheDescriptor cacheDesc = node.context().cache().cacheDescriptor("Person");

            assertNotNull(cacheDesc);

            if (mode == CacheMode.REPLICATED)
                assertEquals(Integer.MAX_VALUE, cacheDesc.cacheConfiguration().getBackups());
            else
                assertEquals(10, cacheDesc.cacheConfiguration().getBackups());

            assertEquals(CacheAtomicityMode.ATOMIC, cacheDesc.cacheConfiguration().getAtomicityMode());

            assertTrue(cacheDesc.sql());

            if (mode != null)
                assertEquals(mode, cacheDesc.cacheConfiguration().getCacheMode());

            QueryTypeDescriptorImpl desc = typeExisting(node, "Person", "Person");

            assertEquals(Object.class, desc.keyClass());
            assertEquals(Object.class, desc.valueClass());

            assertTrue(desc.valueTypeName(), desc.valueTypeName().contains("Person"));

            assertTrue(desc.keyTypeName(), desc.keyTypeName().startsWith(desc.valueTypeName()));
            assertTrue(desc.keyTypeName(), desc.keyTypeName().endsWith("Key"));

            assertEquals(
                F.asList("id", "city", "name", "surname", "age"),
                new ArrayList<>(desc.fields().keySet())
            );

            assertProperty(desc, "id", Integer.class, true);
            assertProperty(desc, "city", String.class, true);
            assertProperty(desc, "name", String.class, false);
            assertProperty(desc, "surname", String.class, false);
            assertProperty(desc, "age", Integer.class, false);

            GridH2Table tbl = ((IgniteH2Indexing)node.context().query().getIndexing()).dataTable("PUBLIC", "Person");

            assertNotNull(tbl);
        }
    }

    /**
     * Test that attempting to specify negative number of backups yields exception.
     */
    public void testNegativeBackups() {
        assertCreateTableWithParamsThrows("bAckUPs = -5  ", "\"BACKUPS\" cannot be negative: -5");
    }

    /**
     * Test that attempting to omit mandatory value of BACKUPS parameter yields an error.
     */
    public void testEmptyBackups() {
        assertCreateTableWithParamsThrows(" bAckUPs =  ", "Parameter value cannot be empty: BACKUPS");
    }

    /**
     * Test that attempting to omit mandatory value of ATOMICITY parameter yields an error.
     */
    public void testEmptyAtomicity() {
        assertCreateTableWithParamsThrows("AtomicitY=  ", "Parameter value cannot be empty: ATOMICITY");
    }

    /**
     * Test that attempting to omit mandatory value of ATOMICITY parameter yields an error.
     */
    public void testInvalidAtomicity() {
        assertCreateTableWithParamsThrows("atomicity=InvalidValue",
            "Invalid value of \"ATOMICITY\" parameter (should be either TRANSACTIONAL or ATOMIC): InvalidValue");
    }

    /**
     * Test that attempting to {@code CREATE TABLE} that already exists does not yield an error if the statement
     *     contains {@code IF NOT EXISTS} clause.
     * @throws Exception if failed.
     */
    public void testCreateTableIfNotExists() throws Exception {
        executeDdl("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
            "\"template=cache\"");

        executeDdl("CREATE TABLE IF NOT EXISTS \"Person\" (\"id\" int, \"city\" varchar," +
            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
            "\"template=cache\"");
    }

    /**
     * Test that attempting to {@code CREATE TABLE} that already exists yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testCreateExistingTable() throws Exception {
        executeDdl("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
            "\"template=cache\"");

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                executeDdl("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar" +
                    ", \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                    "\"template=cache\"");

                return null;
            }
        }, IgniteSQLException.class, "Table already exists: Person");
    }

    /**
     * Test that {@code DROP TABLE} actually removes specified cache and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testDropTable() throws Exception {
        executeDdl("CREATE TABLE IF NOT EXISTS \"Person\" (\"id\" int, \"city\" varchar," +
            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
            "\"template=cache\"");

        executeDdl("DROP TABLE \"Person\"");

        for (int i = 0; i < 4; i++) {
            IgniteEx node = grid(i);

            assertNull(node.cache("Person"));

            QueryTypeDescriptorImpl desc = type(node, "Person", "Person");

            assertNull(desc);
        }
    }

    /**
     * Test that attempting to {@code DROP TABLE} that does not exist does not yield an error if the statement contains
     *     {@code IF EXISTS} clause.
     *
     * @throws Exception if failed.
     */
    public void testDropMissingTableIfExists() throws Exception {
        executeDdl("DROP TABLE IF EXISTS \"City\"");
    }

    /**
     * Test that attempting to {@code DROP TABLE} that does not exist yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDropMissingTable() throws Exception {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                executeDdl("DROP TABLE \"City\"");

                return null;
            }
        }, IgniteSQLException.class, "Table doesn't exist: City");
    }

    /**
     * Check that {@code DROP TABLE} for caches not created with {@code CREATE TABLE} yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDropNonDynamicTable() throws Exception {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                executeDdl("DROP TABLE PUBLIC.\"Integer\"");

                return null;
            }
        }, IgniteSQLException.class,
        "Only cache created with CREATE TABLE may be removed with DROP TABLE [cacheName=cache_idx_2]");
    }

    /**
     * Test that attempting to destroy via cache API a cache created via SQL yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDestroyDynamicSqlCache() throws Exception {
        executeDdl("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
            "\"template=cache\"");

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                client().destroyCache("Person");

                return null;
            }
        }, CacheException.class,
        "Only cache created with cache API may be removed with direct call to destroyCache [cacheName=Person]");
    }

    /**
     * Test that attempting to start a node that has a cache with the name already present in the grid and whose
     * SQL flag does not match that of cache with the same name that is already started, yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testSqlFlagCompatibilityCheck() throws Exception {
        executeDdl("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar, \"name\" varchar, \"surname\" varchar, " +
            "\"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH \"template=cache\"");

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                Ignition.start(clientConfiguration(5).setCacheConfiguration(new CacheConfiguration("Person")));

                return null;
            }
        }, IgniteException.class, "SQL flag mismatch (fix sql flag in cache configuration");
    }

    /**
     * Tests index name conflict check in discovery thread.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testIndexNameConflictCheckDiscovery() throws Exception {
        executeDdl(grid(0), "CREATE TABLE \"Person\" (id int primary key, name varchar)");

        executeDdl(grid(0), "CREATE INDEX \"idx\" ON \"Person\" (\"name\")");

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                QueryEntity e = new QueryEntity();

                e.setTableName("City");
                e.setKeyFields(Collections.singleton("name"));
                e.setFields(new LinkedHashMap<>(Collections.singletonMap("name", String.class.getName())));
                e.setIndexes(Collections.singleton(new QueryIndex("name").setName("idx")));
                e.setValueType("CityKey");
                e.setValueType("City");

                queryProcessor(client()).dynamicTableCreate("PUBLIC", e, CacheMode.PARTITIONED.name(),
                    CacheAtomicityMode.ATOMIC, 10, false);

                return null;
            }
        }, SchemaOperationException.class, "Index already exists: idx");
    }

    /**
     * Tests table name conflict check in {@link DdlStatementsProcessor}.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testTableNameConflictCheckSql() throws Exception {
        executeDdl(grid(0), "CREATE TABLE \"Person\" (id int primary key, name varchar)");

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override  public Object call() throws Exception {
                executeDdl(client(), "CREATE TABLE \"Person\" (id int primary key, name varchar)");

                return null;
            }
        }, IgniteSQLException.class, "Table already exists: Person");
    }

    /**
     * Execute {@code CREATE TABLE} w/given params.
     * @param params Engine parameters.
     */
    private void createTableWithParams(final String params) {
        executeDdl("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar" +
            ", \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
            "\"template=cache," + params + '"');
    }

    /**
     * Test that {@code CREATE TABLE} in non-public schema causes an exception.
     *
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testCreateTableInNonPublicSchema() throws Exception {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                executeDdl("CREATE TABLE \"cache_idx\".\"Person\" (\"id\" int, \"city\" varchar," +
                    " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                    "\"template=cache\"");

                return null;
            }
        }, IgniteSQLException.class, "CREATE TABLE can only be executed on PUBLIC schema.");
    }

    /**
     * Execute {@code CREATE TABLE} w/given params expecting a particular error.
     * @param params Engine parameters.
     * @param expErrMsg Expected error message.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void assertCreateTableWithParamsThrows(final String params, String expErrMsg) {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                createTableWithParams(params);

                return null;
            }
        }, IgniteSQLException.class, expErrMsg);
    }

    /**
     * Test that {@code DROP TABLE} on non-public schema causes an exception.
     *
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDropTableNotPublicSchema() throws Exception {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                executeDdl("DROP TABLE \"cache_idx\".\"Person\"");

                return null;
            }
        }, IgniteSQLException.class, "DROP TABLE can only be executed on PUBLIC schema.");
    }

    /**
     * Execute DDL statement on client node.
     *
     * @param sql Statement.
     */
    private void executeDdl(String sql) {
        executeDdl(client(), sql);
    }

    /**
     * Check that a property in given descriptor is present and has parameters as expected.
     * @param desc Descriptor.
     * @param name Property name.
     * @param type Expected property type.
     * @param isKey {@code true} if the property is expected to belong to key, {@code false} is it's expected to belong
     *     to value.
     */
    private void assertProperty(QueryTypeDescriptorImpl desc, String name, Class<?> type, boolean isKey) {
        GridQueryProperty p = desc.property(name);

        assertNotNull(name, p);

        assertEquals(type, p.type());

        assertEquals(isKey, p.key());
    }

    /**
     * Get configurations to be used in test.
     *
     * @return Configurations.
     * @throws Exception If failed.
     */
    private List<IgniteConfiguration> configurations() throws Exception {
        return Arrays.asList(
            serverConfiguration(0),
            serverConfiguration(1),
            clientConfiguration(2),
            serverConfiguration(3)
        );
    }

    /**
     * Create server configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration serverConfiguration(int idx) throws Exception {
        return commonConfiguration(idx);
    }

    /**
     * Create client configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration clientConfiguration(int idx) throws Exception {
        return commonConfiguration(idx).setClientMode(true);
    }

    /**
     * Create common node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setMarshaller(new BinaryMarshaller());

        return optimize(cfg);
    }

    /**
     * Execute DDL statement on given node.
     *
     * @param node Node.
     * @param sql Statement.
     */
    private void executeDdl(Ignite node, String sql) {
        queryProcessor(node).querySqlFieldsNoCache(new SqlFieldsQuery(sql).setSchema("PUBLIC"), true);
    }

    /**
     * @return Client node.
     */
    private IgniteEx client() {
        return grid(CLIENT);
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setSqlEscapeAll(true);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.PARTITIONED);

        return ccfg;
    }

    /**
     * @return Cache configuration with query entities - unfortunately, we need this to enable indexing at all.
     */
    private CacheConfiguration cacheConfigurationForIndexing() {
        CacheConfiguration<?, ?> ccfg = cacheConfiguration();

        ccfg.setName(INDEXED_CACHE_NAME);

        ccfg.setQueryEntities(Collections.singletonList(
            new QueryEntity()
                .setKeyType(Integer.class.getName())
                .setValueType(Integer.class.getName())
        ));

        return ccfg;
    }

    /**
     * @return Cache configuration with query entities in {@code PUBLIC} schema.
     */
    private CacheConfiguration cacheConfigurationForIndexingInPublicSchema() {
        return cacheConfigurationForIndexing()
            .setName(INDEXED_CACHE_NAME_2)
            .setSqlSchema(QueryUtils.DFLT_SCHEMA)
            .setNodeFilter(F.not(new DynamicIndexAbstractSelfTest.NodeFilter()));
    }
}
