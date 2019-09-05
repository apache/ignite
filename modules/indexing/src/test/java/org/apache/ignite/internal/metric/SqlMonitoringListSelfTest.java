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

package org.apache.ignite.internal.metric;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.internal.processors.metric.list.view.SqlIndexView;
import org.apache.ignite.internal.processors.metric.list.view.SqlSchemaView;
import org.apache.ignite.internal.processors.metric.list.view.SqlTableView;
import org.apache.ignite.internal.processors.query.h2.database.H2IndexType;
import org.apache.ignite.spi.metric.log.LogExporterSpi;
import org.apache.ignite.spi.metric.sql.SqlViewExporterSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheGroupId;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.execute;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SQL_IDXS_MON_LIST;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SQL_IDXS_MON_LIST_DESC;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SQL_SCHEMA_MON_LIST;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SQL_SCHEMA_MON_LIST_DESC;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SQL_TBLS_MON_LIST;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SQL_TBLS_MON_LIST_DESC;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.query.h2.H2TableDescriptor.PK_HASH_IDX_NAME;
import static org.apache.ignite.internal.processors.query.h2.H2TableDescriptor.PK_IDX_NAME;
import static org.apache.ignite.internal.processors.query.h2.opt.H2TableScanIndex.SCAN_INDEX_NAME_SUFFIX;

/** */
public class SqlMonitoringListSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        LogExporterSpi logExporterSpi = new LogExporterSpi();

        logExporterSpi.setPeriod(1_000L);

        cfg.setMetricExporterSpi(new SqlViewExporterSpi(), logExporterSpi);

        return cfg;
    }

    /** */
    @Test
    public void testSchemas() throws Exception {
        try (IgniteEx g = startGrid(new IgniteConfiguration().setSqlSchemas("MY_SCHEMA", "ANOTHER_SCHEMA"))) {
            MonitoringList<String, SqlSchemaView> schemasMonList =
                g.context().metric().list(SQL_SCHEMA_MON_LIST, SQL_SCHEMA_MON_LIST_DESC, SqlSchemaView.class);

            Set<String> schemaFromMon = new HashSet<>();

            schemasMonList.forEach(v -> schemaFromMon.add(v.name()));

            assertEquals(schemaFromMon, new HashSet<>(Arrays.asList("MY_SCHEMA", "ANOTHER_SCHEMA", "SYS", "PUBLIC")));
        }
    }

    /** */
    @Test
    public void testTables() throws Exception {
        try (IgniteEx g = startGrid()) {
            MonitoringList<String, SqlTableView> tblsMonList =
                g.context().metric().list(SQL_TBLS_MON_LIST, SQL_TBLS_MON_LIST_DESC, SqlTableView.class);

            assertEquals(0, tblsMonList.size());

            execute(g, "CREATE TABLE t1(id LONG PRIMARY KEY, NAME VARCHAR) " +
                "WITH \"CACHE_NAME=c1, CACHE_GROUP=g1, VALUE_TYPE=MyType \"");
            execute(g, "CREATE TABLE t2(id LONG PRIMARY KEY, NAME VARCHAR)");
            execute(g, "CREATE TABLE t3(id LONG PRIMARY KEY, NAME VARCHAR)");

            assertEquals(3, tblsMonList.size());

            final boolean[] found = new boolean[1];

            tblsMonList.forEach(v -> {
                if (!"T1".equals(v.tableName()))
                    return;

                found[0] = true;

                assertEquals("c1", v.cacheName());
                assertEquals("g1", v.cacheGroupName());
                assertEquals(cacheGroupId("c1", "g1"), v.cacheGroupId());
                assertEquals(cacheId("c1"), v.cacheId());
                assertEquals("PUBLIC", v.schemaName());
                assertEquals("ID", v.keyAlias());
                assertEquals(Long.class.getName(), v.keyTypeName());
                assertEquals("MyType", v.valueTypeName());
            });

            assertTrue(found[0]);
        }
    }

    /** */
    @Test
    public void testIndexes() throws Exception {
        try (IgniteEx g = startGrid()) {
            MonitoringList<String, SqlIndexView> idxMonList =
                g.context().metric().list(SQL_IDXS_MON_LIST, SQL_IDXS_MON_LIST_DESC, SqlIndexView.class);

            assertEquals(0, idxMonList.size());

            execute(g, "CREATE TABLE t1(id LONG PRIMARY KEY, NAME VARCHAR) WITH \"CACHE_NAME=c1, CACHE_GROUP=g1\"");
            execute(g, "CREATE INDEX name_idx ON t1(name);");

            assertEquals(5, idxMonList.size());

            SqlIndexView idx = idxMonList.get(metricName("PUBLIC", "T1", "NAME_IDX"));

            assertNotNull(idx);
            assertEquals("g1", idx.cacheGroupName());
            assertEquals("c1", idx.cacheName());
            assertEquals(cacheGroupId("c1", "g1"), idx.cacheGroupId());
            assertEquals(cacheId("c1"), idx.cacheId());
            assertEquals("\"NAME\" ASC, \"ID\" ASC", idx.columns());
            assertEquals("NAME_IDX", idx.indexName());
            assertEquals("PUBLIC", idx.schemaName());
            assertEquals("T1", idx.tableName());
            assertEquals(H2IndexType.BTREE, idx.indexType());
            assertFalse(idx.isPk());
            assertFalse(idx.isUnique());

            SqlIndexView pk = idxMonList.get(metricName("PUBLIC", "T1", PK_IDX_NAME));

            assertNotNull(pk);
            assertEquals("g1", pk.cacheGroupName());
            assertEquals("c1", pk.cacheName());
            assertEquals(cacheGroupId("c1", "g1"), pk.cacheGroupId());
            assertEquals(cacheId("c1"), pk.cacheId());
            assertEquals("\"ID\" ASC", pk.columns());
            assertEquals(PK_IDX_NAME, pk.indexName());
            assertEquals("PUBLIC", pk.schemaName());
            assertEquals("T1", pk.tableName());
            assertEquals(H2IndexType.BTREE, pk.indexType());
            assertTrue(pk.isPk());
            assertTrue(pk.isUnique());

            SqlIndexView pkHash = idxMonList.get(metricName("PUBLIC", "T1", PK_HASH_IDX_NAME));

            assertNotNull(pkHash);
            assertEquals("g1", pkHash.cacheGroupName());
            assertEquals("c1", pkHash.cacheName());
            assertEquals(cacheGroupId("c1", "g1"), pkHash.cacheGroupId());
            assertEquals(cacheId("c1"), pkHash.cacheId());
            assertEquals("\"ID\" ASC", pkHash.columns());
            assertEquals(PK_HASH_IDX_NAME, pkHash.indexName());
            assertEquals("PUBLIC", pkHash.schemaName());
            assertEquals("T1", pkHash.tableName());
            assertEquals(H2IndexType.HASH, pkHash.indexType());
            assertTrue(pkHash.isPk());
            assertTrue(pkHash.isUnique());

            SqlIndexView scan = idxMonList.get(metricName("PUBLIC", "T1", PK_IDX_NAME + SCAN_INDEX_NAME_SUFFIX));

            assertNotNull(scan);
            assertEquals("g1", scan.cacheGroupName());
            assertEquals("c1", scan.cacheName());
            assertEquals(cacheGroupId("c1", "g1"), scan.cacheGroupId());
            assertEquals(cacheId("c1"), scan.cacheId());
            assertNull(scan.columns());
            assertEquals(PK_IDX_NAME + SCAN_INDEX_NAME_SUFFIX, scan.indexName());
            assertEquals("PUBLIC", scan.schemaName());
            assertEquals("T1", scan.tableName());
            assertEquals(H2IndexType.SCAN, scan.indexType());
            assertFalse(scan.isPk());
            assertFalse(scan.isUnique());
        }
    }
}
