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
import java.util.List;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlCreateTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;

/**
 * Zzz
 */
public class H2DynamicTableSelfTest extends AbstractSchemaSelfTest {
    /** Client node index. */
    private final static int CLIENT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);

        client().getOrCreateCache(cacheConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        client().destroyCache("Person");

        super.afterTest();
    }

    /** */
    public void testCreateTable() throws Exception {
        cache().query(new SqlFieldsQuery("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
            "\"tplCache=cache\""));

        for (int i = 0; i < 4; i++) {
            IgniteEx node = grid(i);

            assertNotNull(node.cache("Person"));

            QueryTypeDescriptorImpl desc = typeExisting(node, "Person", "Person");

            assertEquals(Object.class, desc.keyClass());

            assertEquals("PersonKey", desc.keyTypeName());

            assertEquals(Object.class, desc.valueClass());

            assertEquals("Person", desc.valueTypeName());

            assertEquals(
                F.asList("id", "city", "name", "surname", "age"),
                new ArrayList<>(desc.fields().keySet())
            );

            assertProperty(desc, "id", Integer.class, true);

            assertProperty(desc, "city", String.class, true);

            assertProperty(desc, "name", String.class, false);

            assertProperty(desc, "surname", String.class, false);

            assertProperty(desc, "age", Integer.class, false);

            GridH2Table tbl = ((IgniteH2Indexing)node.context().query().getIndexing()).dataTable("Person", "Person");

            assertNotNull(tbl);
        }
    }

    /** */
    public void testCreateTableIfNotExists() throws Exception {
        cache().query(new SqlFieldsQuery("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
            "\"tplCache=cache\""));

        cache().query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS \"Person\" (\"id\" int, \"city\" varchar," +
            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
            "\"tplCache=cache\""));
    }

    /** */
    public void testDropTable() throws Exception {
        cache().query(new SqlFieldsQuery("CREATE TABLE IF NOT EXISTS \"Person\" (\"id\" int, \"city\" varchar," +
            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
            "\"tplCache=cache\""));

        cache().query(new SqlFieldsQuery("DROP TABLE \"Person\".\"Person\""));

        for (int i = 0; i < 4; i++) {
            IgniteEx node = grid(i);

            assertNull(node.cache("Person"));

            QueryTypeDescriptorImpl desc = type(node, "Person", "Person");

            assertNull(desc);
        }
    }

    /** */
    public void testDropTableIfExists() throws Exception {
        cache().query(new SqlFieldsQuery("DROP TABLE IF EXISTS \"cache\".\"City\""));
    }

    /** */
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
     * @return Client node.
     */
    private IgniteEx client() {
        return grid(CLIENT);
    }

    /**
     * @return Client node.
     */
    private IgniteCache<?, ?> cache() {
        return client().cache(CACHE_NAME);
    }

    /**
     *
     */
    private JdbcConnection connection() throws Exception {
        GridKernalContext ctx = client().context();

        GridQueryProcessor qryProcessor = ctx.query();

        IgniteH2Indexing idx = U.field(qryProcessor, "idx");

        return (JdbcConnection)idx.connectionForSpace(CACHE_NAME);
    }

    /**
     * @param sql Sql.
     */
    private GridSqlCreateTable parse(String sql) throws Exception {
        Session ses = (Session)connection().getSession();

        return (GridSqlCreateTable)new GridSqlQueryParser(false).parse(ses.prepare(sql));
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

        ccfg.setQueryEntities(Collections.singletonList(
            new QueryEntity()
                .setKeyType(Integer.class.getName())
                .setValueType(Integer.class.getName())
        ));

        return ccfg;
    }
}
