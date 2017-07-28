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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.table.Column;
import org.h2.value.DataType;

/**
 * Common stuff for dynamic columns tests.
 */
public abstract class DynamicColumnsAbstractTest extends GridCommonAbstractTest {
    /** SQL to create test table. */
    private final static String CREATE_SQL = "CREATE TABLE Person (id int primary key, name varchar)";

    /** SQL to drop test table. */
    private final static String DROP_SQL = "DROP TABLE Person";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        run(CREATE_SQL);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        run(DROP_SQL);

        super.afterTest();
    }

    /**
     * @return Node index to run queries on.
     */
    protected abstract int nodeIndex();

    /**
     * Check that given columns have been added to all related structures on target node exactly where needed
     *    (namely, schema in cache descriptor, type descriptor on started cache, and H2 state on started cache).
     * @param node Target node.
     * @param tblName Table name to check.
     * @param afterColName Column after which new columns must be added, or {@code null} if they should be
     *     in the beginning of columns list.
     * @param cols Columns whose presence must be checked.
     */
    static void checkNodeState(IgniteEx node, String tblName, String afterColName, QueryField... cols) {
        String cacheName = QueryUtils.createTableCacheName(QueryUtils.DFLT_SCHEMA, tblName);

        // Schema state check - should pass regardless of cache state.
        {
            DynamicCacheDescriptor desc = node.context().cache().cacheDescriptor(cacheName);

            assertNotNull("Cache descriptor not found", desc);

            assertTrue(desc.sql());

            QuerySchema schema = desc.schema();

            assertNotNull(schema);

            QueryEntity entity = null;

            for (QueryEntity e : schema.entities()) {
                if (F.eq(tblName, e.getTableName())) {
                    entity = e;

                    break;
                }
            }

            assertNotNull("Query entity not found", entity);

            Iterator<Map.Entry<String, String>> it = entity.getFields().entrySet().iterator();

            if (!F.isEmpty(afterColName)) {
                while (it.hasNext()) {
                    Map.Entry<String, String> e = it.next();

                    if (F.eq(afterColName, e.getKey()))
                        break;
                }
            }

            for (QueryField col : cols) {
                assertTrue("New column not found in query entity: " + col.name(), it.hasNext());

                Map.Entry<String, String> e = it.next();

                assertEquals(col.name(), e.getKey());

                assertEquals(col.typeName(), e.getValue());
            }
        }

        // Start cache on this node if we haven't yet.
        node.cache(cacheName);

        // Type descriptor state check.
        {
            Collection<GridQueryTypeDescriptor> descs = node.context().query().types(cacheName);

            GridQueryTypeDescriptor desc = null;

            for (GridQueryTypeDescriptor d : descs) {
                if (F.eq(tblName, d.tableName())) {
                    desc = d;

                    break;
                }
            }

            assertNotNull("Type descriptor not found", desc);

            Iterator<Map.Entry<String, Class<?>>> it = desc.fields().entrySet().iterator();

            if (!F.isEmpty(afterColName)) {
                while (it.hasNext()) {
                    Map.Entry<String, Class<?>> e = it.next();

                    if (F.eq(afterColName, e.getKey()))
                        break;
                }
            }

            for (QueryField col : cols) {
                assertTrue("New column not found in type descriptor: " + col.name(), it.hasNext());

                Map.Entry<String, Class<?>> e = it.next();

                assertEquals(col.name(), e.getKey());

                assertEquals(col.typeName(), e.getValue().getName());
            }
        }



        // H2 table state check.
        {
            GridH2Table tbl = ((IgniteH2Indexing)node.context().query().getIndexing()).dataTable(QueryUtils.DFLT_SCHEMA,
                tblName);

            assertNotNull("Table not found", tbl);

            Iterator<Column> it = Arrays.asList(tbl.getColumns()).iterator();

            for (int i = 0; i < 3; i++)
                it.next();

            if (!F.isEmpty(afterColName)) {
                while (it.hasNext()) {
                    Column c = it.next();

                    if (F.eq(afterColName, c.getName()))
                        break;
                }
            }

            for (QueryField col : cols) {
                assertTrue("New column not found in H2 table: " + col.name(), it.hasNext());

                assertTrue(it.hasNext());

                Column c = it.next();

                assertEquals(col.name(), c.getName());

                assertEquals(col.typeName(), DataType.getTypeClassName(c.getType()));
            }
        }
    }

    /**
     * @param name New column name.
     * @param typeName Class name for this new column's data type.
     * @return New column with given name and type.
     */
    protected static QueryField c(String name, String typeName) {
        return new QueryField(name, typeName);
    }

    /**
     * Run specified statement expected to throw {@code IgniteSqlException} with expected specified message.
     * @param sql Statement.
     * @param msg Expected message.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    protected void assertThrows(final String sql, String msg) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                run(sql);

                return null;
            }
        }, IgniteSQLException.class, msg);
    }

    /**
     * @param idx Node index.
     * @return Client configuration.
     * @throws Exception if failed.
     */
    protected IgniteConfiguration clientConfiguration(int idx) throws Exception {
        return commonConfiguration(idx).setClientMode(true).setCacheConfiguration(
            new CacheConfiguration<>("idx").setIndexedTypes(Integer.class, Integer.class)
        );
    }

    /**
     * Create common node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        MemoryConfiguration memCfg = new MemoryConfiguration()
            .setDefaultMemoryPolicyName("default")
            .setMemoryPolicies(
                new MemoryPolicyConfiguration()
                    .setName("default")
                    .setMaxSize(32 * 1024 * 1024L)
                    .setInitialSize(32 * 1024 * 1024L)
            );

        cfg.setMemoryConfiguration(memCfg);

        return optimize(cfg);
    }

    /**
     * Create server node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    IgniteConfiguration serverConfiguration(int idx) throws Exception {
        return commonConfiguration(idx);
    }

    /**
     * Execute SQL command and ignore resulting dataset.
     * @param sql Statement.
     */
    protected void run(String sql) {
        grid(nodeIndex()).context().query()
            .querySqlFieldsNoCache(new SqlFieldsQuery(sql).setSchema(QueryUtils.DFLT_SCHEMA), true).getAll();
    }
}
