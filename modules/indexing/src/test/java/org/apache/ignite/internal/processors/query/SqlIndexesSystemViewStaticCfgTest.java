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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class SqlIndexesSystemViewStaticCfgTest extends GridCommonAbstractTest {
    /** */
    private Ignite driver;

    /** */
    private boolean client;

    /** */
    private CacheConfiguration<Object, Object>[] ccfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClientMode(client)
            .setCacheConfiguration(ccfg)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    public void testNotActivatedGrid() throws Exception {
        ccfg = (CacheConfiguration<Object, Object>[])new CacheConfiguration[] {
            new CacheConfiguration<>("cache")
                .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, TestValue.class)
                .setIndexes(Collections.singleton(new QueryIndex("i")))))
        };

        // start server
        startGrid(0);

        // start client
        client = true;

        startGrid(2);

        for (Ignite ign : G.allGrids()) {
            SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM IGNITE.INDEXES ORDER BY TABLE_NAME, INDEX_NAME");

            GridTestUtils.assertThrowsWithCause(
                () -> ign.cache("cache").query(qry).getAll(),
                IgniteException.class);
        }
    }

    /** */
    public void testIndexesViewDisabledBySystemProperty() throws Exception {
        String old = System.getProperty(IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS);

        System.setProperty(IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS, "true");

        try {
            ccfg = (CacheConfiguration<Object, Object>[])new CacheConfiguration[] {
                new CacheConfiguration<>("cache")
                    .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, TestValue.class)
                    .setIndexes(Collections.singleton(new QueryIndex("i")))))
            };

            startNodes();

            for (Ignite ign : G.allGrids()) {
                SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM IGNITE.INDEXES ORDER BY TABLE_NAME, INDEX_NAME");

                Throwable e = GridTestUtils.assertThrowsWithCause(
                    () -> ign.cache("cache").query(qry).getAll(),
                    IgniteSQLException.class);

                assertTrue(e.getMessage().contains("Schema \"IGNITE\" not found"));
            }
        }
        finally {
            if (old == null)
                System.clearProperty(IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS);
            else
                System.setProperty(IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS, old);
        }
    }

    /** */
    public void testStaticCacheCfg() throws Exception {
        ccfg = (CacheConfiguration<Object, Object>[])new CacheConfiguration[] {
            new CacheConfiguration<>("cache")
                .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, TestValue.class)
                .setIndexes(Collections.singleton(new QueryIndex("i")))))
        };

        startNodes();

        List<Object> expCache = Arrays.asList(
            Arrays.asList("cache", "TESTVALUE", "TESTVALUE_I_ASC_IDX", "\"I\" ASC, \"_KEY\" ASC", "BTREE", false, false, 94416770, "cache", 94416770, "cache", 10),
            Arrays.asList("cache", "TESTVALUE", "__SCAN_", null, "SCAN", false, false, 94416770, "cache", 94416770, "cache", null),
            Arrays.asList("cache", "TESTVALUE", "_key_PK", "\"_KEY\" ASC", "BTREE", true, true, 94416770, "cache", 94416770, "cache", 5),
            Arrays.asList("cache", "TESTVALUE", "_key_PK_hash", "\"_KEY\" ASC", "HASH", false, true, 94416770, "cache", 94416770, "cache", null)
        );

        checkIndexes(expCache::equals);

        driver.destroyCache("cache");

        checkIndexes(List::isEmpty);
    }

    /** */
    public void testStaticCacheInGroupCfg() throws Exception {
        ccfg = (CacheConfiguration<Object, Object>[])new CacheConfiguration[] {
            new CacheConfiguration<>("cache1")
                .setGroupName("group")
                .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, TestValue.class)
                .setIndexes(Collections.singleton(new QueryIndex("i"))))),
            new CacheConfiguration<>("cache2")
                .setGroupName("group")
        };

        startNodes();

        List<Object> expGrp = Arrays.asList(
            Arrays.asList("cache1", "TESTVALUE", "TESTVALUE_I_ASC_IDX", "\"I\" ASC, \"_KEY\" ASC", "BTREE", false, false, -1368047377, "cache1", 98629247, "group", 10),
            Arrays.asList("cache1", "TESTVALUE", "__SCAN_", null, "SCAN", false, false, -1368047377, "cache1", 98629247, "group", null),
            Arrays.asList("cache1", "TESTVALUE", "_key_PK", "\"_KEY\" ASC", "BTREE", true, true, -1368047377, "cache1", 98629247, "group", 5),
            Arrays.asList("cache1", "TESTVALUE", "_key_PK_hash", "\"_KEY\" ASC", "HASH", false, true, -1368047377, "cache1", 98629247, "group", null)
        );

        checkIndexes(expGrp::equals);

        driver.destroyCache("cache1");

        checkIndexes(List::isEmpty);
    }

    /** */
    private void startNodes() throws Exception {
        // baseline node
        driver = startGrid(0);

        driver.cluster().active(true);
        driver.cluster().setBaselineTopology(Collections.singleton(grid(0).localNode()));

        // node out of baseline
        startGrid(1);

        // client node
        G.setClientMode(true);
        startGrid(3);
    }

    /** */
    private void checkIndexes(Predicate<List<List<?>>> checker) throws Exception {
        for (Ignite ign : G.allGrids()) {
            assertTrue(GridTestUtils.waitForCondition(() -> {
                List<List<?>> indexes = execSql(ign, "SELECT * FROM IGNITE.INDEXES ORDER BY INDEX_NAME");

                return checker.test(indexes);
            }, 1000));
        }
    }

    /** */
    private static List<List<?>> execSql(Ignite ign, String sql) {
        return ((IgniteEx)ign).context().query().querySqlFields(new SqlFieldsQuery(sql), false).getAll();
    }

    /** */
    public static class TestValue {
        /** */
        @QuerySqlField
        int i;
    }
}
