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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheConfigurations;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheNames;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests that DDL requests works fine, if cluster in a {@link ClusterState#ACTIVE_READ_ONLY} state.
 */
public class GridCacheSqlDdlClusterReadOnlyModeTest extends CacheCreateDestroyClusterReadOnlyModeAbstractTest {
    /** Sql select query error message. */
    private static final String SQL_SELECT_ERROR_MSG = "Failed to parse query. Column \"CITY\" not found";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /** */
    @Test
    public void testCreateTableDenied() {
        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        for (String ddl : generateCreateTableDDL()) {
            for (Ignite node : G.allGrids()) {
                Throwable t = assertThrows(log, () -> execute(node, ddl), Exception.class, null);

                ClusterReadOnlyModeTestUtils.checkRootCause(t, node.name() + " sql: " + ddl);
            }
        }
    }

    /** */
    @Test
    public void testDropTableDenied() {
        createTables();

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        for (String cacheName : cacheNames()) {
            String sql = "drop table " + tableName(cacheName);

            for (Ignite node : G.allGrids()) {
                Throwable t = assertThrows(log, () -> execute(node, sql), Exception.class, null);

                ClusterReadOnlyModeTestUtils.checkRootCause(t, node.name() + " sql: " + sql);
            }
        }
    }

    /** */
    @Test
    public void testCreateDropIndexAllowed() {
        createTables();

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        for (String cacheName : cacheNames()) {
            for (Ignite node : G.allGrids()) {
                String indexName = "age_idx_" + tableName(cacheName);
                assertNotNull(execute(node, "create index " + indexName + " on " + tableName(cacheName) + " (age)"));

                assertNotNull(execute(node, "drop index " + indexName));
            }
        }
    }

    /** */
    @Test
    public void testAlterTableAllowed() {
        createTables();

        grid(0).cluster().state(ClusterState.ACTIVE_READ_ONLY);

        for (CacheConfiguration cfg : cacheConfigurations()) {
            String cacheName = cfg.getName();

            if (cfg.getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT) {
                // Drop column doesn't support in MVCC mode.
                continue;
            }

            for (Ignite node : G.allGrids()) {
                String selectSql = "select city from " + tableName(cacheName);

                assertThrows(log, () -> execute(node, selectSql), IgniteSQLException.class, SQL_SELECT_ERROR_MSG);

                execute(node, "alter table " + tableName(cacheName) + " add column city varchar");

                assertNotNull(execute(node, selectSql));

                execute(node, "alter table " + tableName(cacheName) + " drop column city");

                assertThrows(log, () -> execute(node, selectSql), IgniteSQLException.class, SQL_SELECT_ERROR_MSG);
            }
        }
    }

    /** */
    private static List<String> generateCreateTableDDL() {
        List<String> ddls = new ArrayList<>();

        for (CacheConfiguration cfg : cacheConfigurations()) {
            SB sb = new SB("CREATE TABLE ");

            sb.a(tableName(cfg.getName()));
            sb.a(" (id int, city_id int, age int, PRIMARY KEY (id)) WITH \"");
            sb.a("backups=").a(cfg.getBackups()).a(",");
            sb.a("CACHE_NAME=").a(cfg.getName()).a(",");

            if (cfg.getGroupName() != null)
                sb.a("CACHE_GROUP=").a(cfg.getGroupName()).a(",");

            sb.a("ATOMICITY=").a(cfg.getAtomicityMode()).a(",");
            sb.a("TEMPLATE=").a(cfg.getCacheMode());
            sb.a("\"");

            ddls.add(sb.toString());
        }
        return ddls;
    }

    /** */
    private void createTables() {
        for (String ddl : generateCreateTableDDL())
            execute(grid(0), ddl);

        for (String cacheName : cacheNames())
            execute(grid(0), "INSERT INTO " + tableName(cacheName) + " (id, city_id, age) VALUES (1, 1, 1)");
    }

    /** */
    private static Object execute(Ignite node, String sql, Object... args) {
        return node.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql).setArgs(args)).getAll();
    }

    /** */
    private static String tableName(String cacheName) {
        return "tbl_" + cacheName;
    }
}
