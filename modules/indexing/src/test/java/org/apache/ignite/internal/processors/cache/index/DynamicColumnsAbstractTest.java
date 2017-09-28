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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
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
import org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
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
    final static String CREATE_SQL = "CREATE TABLE IF NOT EXISTS Person (id int primary key, name varchar)";

    /** SQL to drop test table. */
    final static String DROP_SQL = "DROP TABLE Person";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * Check that given columns have been added to all related structures on target node exactly where needed
     *    (namely, schema in cache descriptor, type descriptor on started cache, and H2 state on started cache).
     * @param node Target node.
     * @param schemaName Schema name to look for the table in.
     * @param tblName Table name to check.
     * @param cols Columns whose presence must be checked.
     */
    static void checkNodeState(IgniteEx node, String schemaName, String tblName, QueryField... cols) {
        String cacheName = F.eq(schemaName, QueryUtils.DFLT_SCHEMA) ?
            QueryUtils.createTableCacheName(schemaName, tblName) : schemaName;

        // Schema state check - should pass regardless of cache state.
        {
            DynamicCacheDescriptor desc = node.context().cache().cacheDescriptor(cacheName);

            assertNotNull("Cache descriptor not found", desc);

            assertTrue(desc.sql() == F.eq(schemaName, QueryUtils.DFLT_SCHEMA));

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

            for (int i = entity.getFields().size() - cols.length; i > 0 && it.hasNext(); i--)
                it.next();

            for (QueryField col : cols) {
                assertTrue("New column not found in query entity: " + col.name(), it.hasNext());

                Map.Entry<String, String> e = it.next();

                assertEquals(col.name(), e.getKey());

                assertEquals(col.typeName(), e.getValue());

                if (!col.isNullable()) {
                    assertNotNull(entity.getNotNullFields());

                    assertTrue(entity.getNotNullFields().contains(col.name()));
                }
                else if (entity.getNotNullFields() != null)
                    assertFalse(entity.getNotNullFields().contains(col.name()));
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

            for (int i = desc.fields().size() - cols.length; i > 0 && it.hasNext(); i--)
                it.next();

            for (QueryField col : cols) {
                assertTrue("New column not found in type descriptor: " + col.name(), it.hasNext());

                Map.Entry<String, Class<?>> e = it.next();

                assertEquals(col.name(), e.getKey());

                assertEquals(col.typeName(), e.getValue().getName());

                assertTrue(col.isNullable() || desc.property(col.name()).notNull());
            }
        }

        // H2 table state check.
        {
            GridH2Table tbl = ((IgniteH2Indexing)node.context().query().getIndexing()).dataTable(schemaName,
                tblName);

            assertNotNull("Table not found", tbl);

            Iterator<Column> colIt = Arrays.asList(tbl.getColumns()).iterator();

            GridH2RowDescriptor rowDesc = tbl.rowDescriptor();

            int i = 0;

            for (; i < tbl.getColumns().length - cols.length && colIt.hasNext(); i++)
                colIt.next();

            for (QueryField col : cols) {
                assertTrue("New column not found in H2 table: " + col.name(), colIt.hasNext());

                assertTrue(colIt.hasNext());

                Column c = colIt.next();

                assertEquals(col.name(), c.getName());

                assertEquals(col.typeName(), DataType.getTypeClassName(c.getType()));

                assertFalse(rowDesc.isKeyValueOrVersionColumn(i));

                assertEquals(col.isNullable(), c.isNullable());

                try {
                    assertEquals(DataType.getTypeFromClass(Class.forName(col.typeName())),
                        rowDesc.fieldType(i - GridH2KeyValueRowOnheap.DEFAULT_COLUMNS_COUNT));
                }
                catch (ClassNotFoundException e) {
                    throw new AssertionError(e);
                }

                i++;
            }
        }
    }

    /**
     * Check that given columns have been added to all related structures on all started nodes (namely, schema
     *     in cache descriptor, type descriptor on started cache, and H2 state on started cache).
     * @param tblName Table name to check.
     * @param cols Columns whose presence must be checked.
     */
    static void checkNodesState(String tblName, QueryField... cols) {
        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, QueryUtils.DFLT_SCHEMA, tblName, cols);
    }

    /**
     * @param name New column name.
     * @param typeName Class name for this new column's data type.
     * @return New column with given name and type.
     */
    protected static QueryField c(String name, String typeName) {
        return new QueryField(name, typeName, true);
    }

    /**
     * @param idx Node index.
     * @return Client configuration.
     * @throws Exception if failed.
     */
    protected IgniteConfiguration clientConfiguration(int idx) throws Exception {
        QueryEntity e = new QueryEntity(Integer.class.getName(), "Person");

        LinkedHashMap<String, String> flds = new LinkedHashMap<>();

        flds.put("name", String.class.getName());

        e.setFields(flds);

        return commonConfiguration(idx).setClientMode(true).setCacheConfiguration(
            new CacheConfiguration<>("idx").setQueryEntities(Collections.singletonList(e))
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
                    .setMaxSize(128 * 1024 * 1024L)
                    .setInitialSize(128 * 1024 * 1024L)
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
     * Execute SQL command and return resulting dataset.
     * @param node Node to run query from.
     * @param sql Statement.
     * @return result.
     */
    protected List<List<?>> run(Ignite node, String sql) {
        return ((IgniteEx)node).context().query()
            .querySqlFieldsNoCache(new SqlFieldsQuery(sql).setSchema(QueryUtils.DFLT_SCHEMA), true).getAll();
    }

    /**
     * Execute SQL command and return resulting dataset.
     * @param cache Cache to initiate query from.
     * @param sql Statement.
     * @return result.
     */
    protected List<List<?>> run(IgniteCache<?, ?> cache, String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema(QueryUtils.DFLT_SCHEMA).setArgs(args)
            .setDistributedJoins(true);

        return cache.query(qry).getAll();
    }

    /**
     * Run specified statement expected to throw {@code IgniteSqlException} with expected specified message.
     * @param sql Statement.
     * @param msg Expected message.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    protected void assertThrows(final Ignite node, final String sql, String msg) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                run(node, sql);

                return null;
            }
        }, IgniteSQLException.class, msg);
    }
}
