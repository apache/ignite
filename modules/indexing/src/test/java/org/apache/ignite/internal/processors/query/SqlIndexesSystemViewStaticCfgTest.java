/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
    @Test
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
            GridTestUtils.assertThrowsWithCause(
                () -> execSql(ign, "SELECT * FROM SYS.INDEXES ORDER BY TABLE_NAME, INDEX_NAME"),
                IgniteException.class);
        }
    }

    /** */
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_SQL_DISABLE_SYSTEM_VIEWS, value = "true")
    @Test
    public void testIndexesViewDisabledBySystemProperty() throws Exception {
        ccfg = (CacheConfiguration<Object, Object>[])new CacheConfiguration[] {
            new CacheConfiguration<>("cache")
                .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, TestValue.class)
                .setIndexes(Collections.singleton(new QueryIndex("i")))))
        };

        startNodes();

        for (Ignite ign : G.allGrids()) {
            Throwable e = GridTestUtils.assertThrowsWithCause(
                () -> execSql(ign, "SELECT * FROM SYS.INDEXES ORDER BY TABLE_NAME, INDEX_NAME"),
                IgniteSQLException.class);

            assertTrue(e.getMessage().contains("Schema \"SYS\" not found"));
        }
    }

    /** */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testStaticCacheCfg() throws Exception {
        ccfg = (CacheConfiguration<Object, Object>[])new CacheConfiguration[] {
            new CacheConfiguration<>("cache")
                .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, TestValue.class)
                .setIndexes(Collections.singleton(new QueryIndex("i")))))
        };

        startNodes();

        List<Object> expCache = Arrays.asList(
            Arrays.asList(94416770, "cache", 94416770, "cache", "cache", "TESTVALUE", "TESTVALUE_I_ASC_IDX", "BTREE", "\"I\" ASC, \"_KEY\" ASC", false, false, 10),
            Arrays.asList(94416770, "cache", 94416770, "cache", "cache", "TESTVALUE", "__SCAN_", "SCAN", null, false, false, null),
            Arrays.asList(94416770, "cache", 94416770, "cache", "cache", "TESTVALUE", "_key_PK", "BTREE", "\"_KEY\" ASC", true, true, 5),
            Arrays.asList(94416770, "cache", 94416770, "cache", "cache", "TESTVALUE", "_key_PK_hash", "HASH", "\"_KEY\" ASC", false, true, null)
        );

        checkIndexes(expCache::equals);

        driver.cluster().active(false);

        for (Ignite ign : G.allGrids()) {
            GridTestUtils.assertThrowsWithCause(
                () -> execSql(ign, "SELECT * FROM SYS.INDEXES ORDER BY TABLE_NAME, INDEX_NAME"),
                IgniteException.class);
        }

        driver.cluster().active(true);

        checkIndexes(expCache::equals);

        driver.destroyCache("cache");

        checkIndexes(List::isEmpty);
    }

    /** */
    @Test
    public void testStaticCacheInGroupCfg() throws Exception {
        ccfg = (CacheConfiguration<Object, Object>[])new CacheConfiguration[] {
            new CacheConfiguration<>("cache1")
                .setGroupName("group")
                .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, TestValue.class)
                .setIndexes(Collections.singleton(new QueryIndex("i"))))),
            new CacheConfiguration<>("cache2")
                .setGroupName("group")
                .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, TestValue.class)
                .setIndexes(Collections.singleton(new QueryIndex("i")))))
        };

        startNodes();

        List<Object> expGrpBoth = Arrays.asList(
            Arrays.asList(98629247, "group", -1368047377, "cache1", "cache1", "TESTVALUE", "TESTVALUE_I_ASC_IDX", "BTREE", "\"I\" ASC, \"_KEY\" ASC", false, false, 10),
            Arrays.asList(98629247, "group", -1368047377, "cache1", "cache1", "TESTVALUE", "__SCAN_", "SCAN", null, false, false, null),
            Arrays.asList(98629247, "group", -1368047377, "cache1", "cache1", "TESTVALUE", "_key_PK", "BTREE", "\"_KEY\" ASC", true, true, 5),
            Arrays.asList(98629247, "group", -1368047377, "cache1", "cache1", "TESTVALUE", "_key_PK_hash", "HASH", "\"_KEY\" ASC", false, true, null),

            Arrays.asList(98629247, "group", -1368047376, "cache2", "cache2", "TESTVALUE", "TESTVALUE_I_ASC_IDX", "BTREE", "\"I\" ASC, \"_KEY\" ASC", false, false, 10),
            Arrays.asList(98629247, "group", -1368047376, "cache2", "cache2", "TESTVALUE", "__SCAN_", "SCAN", null, false, false, null),
            Arrays.asList(98629247, "group", -1368047376, "cache2", "cache2", "TESTVALUE", "_key_PK", "BTREE", "\"_KEY\" ASC", true, true, 5),
            Arrays.asList(98629247, "group", -1368047376, "cache2", "cache2", "TESTVALUE", "_key_PK_hash", "HASH", "\"_KEY\" ASC", false, true, null)
        );

        List<Object> expGrpSingle = Arrays.asList(
            Arrays.asList(98629247, "group", -1368047376, "cache2", "cache2", "TESTVALUE", "TESTVALUE_I_ASC_IDX", "BTREE", "\"I\" ASC, \"_KEY\" ASC", false, false, 10),
            Arrays.asList(98629247, "group", -1368047376, "cache2", "cache2", "TESTVALUE", "__SCAN_", "SCAN", null, false, false, null),
            Arrays.asList(98629247, "group", -1368047376, "cache2", "cache2", "TESTVALUE", "_key_PK", "BTREE", "\"_KEY\" ASC", true, true, 5),
            Arrays.asList(98629247, "group", -1368047376, "cache2", "cache2", "TESTVALUE", "_key_PK_hash", "HASH", "\"_KEY\" ASC", false, true, null)
        );

        checkIndexes(expGrpBoth::equals);

        driver.destroyCache("cache1");

        checkIndexes(expGrpSingle::equals);
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
                List<List<?>> indexes = execSql(ign, "SELECT * FROM SYS.INDEXES ORDER BY CACHE_NAME, INDEX_NAME");

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
