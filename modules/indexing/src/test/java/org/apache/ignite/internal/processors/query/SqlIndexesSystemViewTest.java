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
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class SqlIndexesSystemViewTest extends GridCommonAbstractTest {
    /** */
    private Ignite driver;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClientMode(client)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // baseline node
        driver = startGrid(0);

        driver.cluster().active(true);
        driver.cluster().setBaselineTopology(Collections.singleton(grid(0).localNode()));

        // node out of baseline
        startGrid(1);

        // client node
        client = true;

        startGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    public void testNoTablesNoIndexes() throws Exception {
        checkIndexes(List::isEmpty);
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    public void testDdlTableIndexes() throws Exception {
        execSql(driver, "CREATE TABLE Person(id INT PRIMARY KEY, name VARCHAR, age INT)");

        List<Object> expInit = Arrays.asList(
            Arrays.asList("PUBLIC", "PERSON", "__SCAN_", null, "SCAN", false, false, -1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", null),
            Arrays.asList("PUBLIC", "PERSON", "_key_PK", "\"_KEY\" ASC", "BTREE", true, true, -1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", 5),
            Arrays.asList("PUBLIC", "PERSON", "_key_PK_hash", "\"_KEY\" ASC", "HASH", false, true, -1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", null)
        );

        checkIndexes(expInit::equals);

        driver.cluster().active(false);

        // Indexes view is empty for inactive cluster in 2.5
        checkIndexes(List::isEmpty);

        driver.cluster().active(true);

        checkIndexes(expInit::equals);

        try {
            execSql(driver, "CREATE INDEX NameIdx ON Person(surname)");

            fail("Exception expected");
        }
        catch (IgniteSQLException ignored) {
        }

        checkIndexes(expInit::equals);

        execSql(driver, "CREATE INDEX NameIdx ON Person(name)");

        List<Object> expWithSecondary = Arrays.asList(
            Arrays.asList("PUBLIC", "PERSON", "NAMEIDX", "\"NAME\" ASC, \"_KEY\" ASC", "BTREE", false, false, -1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", 10),
            Arrays.asList("PUBLIC", "PERSON", "__SCAN_", null, "SCAN", false, false, -1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", null),
            Arrays.asList("PUBLIC", "PERSON", "_key_PK", "\"_KEY\" ASC", "BTREE", true, true, -1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", 5),
            Arrays.asList("PUBLIC", "PERSON", "_key_PK_hash", "\"_KEY\" ASC", "HASH", false, true, -1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", null)
        );

        checkIndexes(expWithSecondary::equals);

        execSql(driver, "DROP INDEX NameIdx");

        checkIndexes(expInit::equals);

        execSql(driver, "CREATE INDEX CompIdx ON Person(name, age)");

        List<Object> expWithCompound = Arrays.asList(
            Arrays.asList("PUBLIC", "PERSON", "COMPIDX", "\"NAME\" ASC, \"AGE\" ASC, \"_KEY\" ASC", "BTREE", false, false, -1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", 10),
            Arrays.asList("PUBLIC", "PERSON", "__SCAN_", null, "SCAN", false, false, -1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", null),
            Arrays.asList("PUBLIC", "PERSON", "_key_PK", "\"_KEY\" ASC", "BTREE", true, true, -1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", 5),
            Arrays.asList("PUBLIC", "PERSON", "_key_PK_hash", "\"_KEY\" ASC", "HASH", false, true, -1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", null)
        );

        checkIndexes(expWithCompound::equals);

        execSql(driver, "DROP TABLE Person");

        checkIndexes(List::isEmpty);
    }

    /** */
    public void testDdlTableIndexes2() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-11125");

        execSql(driver, "CREATE TABLE Person(id1 INT, id2 INT, name VARCHAR, age INT, PRIMARY KEY (id1, id2))");

        List<Object> expInit = Arrays.asList(
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "__SCAN_", "SCAN", null, false, false, null),
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "_key_PK", "BTREE", "\"ID1\" ASC, \"ID2\" ASC", true, true, 10),
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "_key_PK_hash", "HASH", "\"ID1\" ASC, \"ID2\" ASC", false, true, null)
        );

        checkIndexes(expInit::equals);

        execSql(driver, "CREATE INDEX CompIdx ON Person(name, age)");

        List<Object> expWithSecondary = Arrays.asList(
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "COMPIDX", "BTREE", "\"NAME\" ASC, \"AGE\" ASC, \"ID1\" ASC, \"ID2\" ASC", false, false, 10),
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "__SCAN_", "SCAN", null, false, false, null),
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "_key_PK", "BTREE", "\"ID1\" ASC, \"ID2\" ASC", true, true, 10),
            Arrays.asList(-1447683814, "SQL_PUBLIC_PERSON", -1447683814, "SQL_PUBLIC_PERSON", "PUBLIC", "PERSON", "_key_PK_hash", "HASH", "\"ID1\" ASC, \"ID2\" ASC", false, true, null)
        );

        checkIndexes(expWithSecondary::equals);
    }

    /** */
    public void testQueryEntityIndexes() throws Exception {
        driver.createCache(new CacheConfiguration<>("cache")
            .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, TestValue.class)
                .setIndexes(Collections.singleton(new QueryIndex("i"))))));

        List<Object> expCache = Arrays.asList(
            Arrays.asList("cache", "TESTVALUE", "TESTVALUE_I_ASC_IDX", "\"I\" ASC, \"_KEY\" ASC", "BTREE", false, false, 94416770, "cache", 94416770, "cache", 10),
            Arrays.asList("cache", "TESTVALUE", "__SCAN_", null, "SCAN", false, false, 94416770, "cache", 94416770, "cache", null),
            Arrays.asList("cache", "TESTVALUE", "_key_PK", "\"_KEY\" ASC", "BTREE", true, true, 94416770, "cache", 94416770, "cache", 5),
            Arrays.asList("cache", "TESTVALUE", "_key_PK_hash", "\"_KEY\" ASC", "HASH", false, true, 94416770, "cache", 94416770, "cache", null)
        );

        checkIndexes(expCache::equals);

        driver.destroyCache("cache");

        checkIndexes(List::isEmpty);

        List<Object> expGrpBoth = Arrays.asList(
            Arrays.asList("cache1", "TESTVALUE", "TESTVALUE_I_ASC_IDX", "\"I\" ASC, \"_KEY\" ASC", "BTREE", false, false, -1368047377, "cache1", 98629247, "group", 10),
            Arrays.asList("cache1", "TESTVALUE", "__SCAN_", null, "SCAN", false, false, -1368047377, "cache1", 98629247, "group", null),
            Arrays.asList("cache1", "TESTVALUE", "_key_PK", "\"_KEY\" ASC", "BTREE", true, true, -1368047377, "cache1", 98629247, "group", 5),
            Arrays.asList("cache1", "TESTVALUE", "_key_PK_hash", "\"_KEY\" ASC", "HASH", false, true, -1368047377, "cache1", 98629247, "group", null),

            Arrays.asList("cache2", "TESTVALUE", "TESTVALUE_I_ASC_IDX", "\"I\" ASC, \"_KEY\" ASC", "BTREE", false, false, -1368047376, "cache2", 98629247, "group", 10),
            Arrays.asList("cache2", "TESTVALUE", "__SCAN_", null, "SCAN", false, false, -1368047376, "cache2", 98629247, "group", null),
            Arrays.asList("cache2", "TESTVALUE", "_key_PK", "\"_KEY\" ASC", "BTREE", true, true, -1368047376, "cache2", 98629247, "group", 5),
            Arrays.asList("cache2", "TESTVALUE", "_key_PK_hash", "\"_KEY\" ASC", "HASH", false, true, -1368047376, "cache2", 98629247, "group", null)
        );

        List<Object> expGrpSingle = Arrays.asList(
            Arrays.asList("cache2", "TESTVALUE", "TESTVALUE_I_ASC_IDX", "\"I\" ASC, \"_KEY\" ASC", "BTREE", false, false, -1368047376, "cache2", 98629247, "group", 10),
            Arrays.asList("cache2", "TESTVALUE", "__SCAN_", null, "SCAN", false, false, -1368047376, "cache2", 98629247, "group", null),
            Arrays.asList("cache2", "TESTVALUE", "_key_PK", "\"_KEY\" ASC", "BTREE", true, true, -1368047376, "cache2", 98629247, "group", 5),
            Arrays.asList("cache2", "TESTVALUE", "_key_PK_hash", "\"_KEY\" ASC", "HASH", false, true, -1368047376, "cache2", 98629247, "group", null)
        );

        driver.createCache(new CacheConfiguration<>("cache1")
            .setGroupName("group")
            .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, TestValue.class)
                .setIndexes(Collections.singleton(new QueryIndex("i"))))));

        driver.createCache(new CacheConfiguration<>("cache2")
            .setGroupName("group")
            .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, TestValue.class)
                .setIndexes(Collections.singleton(new QueryIndex("i"))))));

        checkIndexes(expGrpBoth::equals);

        driver.destroyCache("cache1");

        checkIndexes(expGrpSingle::equals);
    }

    /** */
    public void testTextIndex() throws Exception {
        IgniteCache<Object, Object> cache = driver.createCache(new CacheConfiguration<>("cache")
            .setQueryEntities(Collections.singleton(new QueryEntity(Integer.class, String.class))));

        cache.put(1, "john");
        cache.put(2, "jack");

        List<T2<Integer, String>> res = cache.query(new TextQuery<Integer, String>("String", "john")).getAll().stream()
            .map(e -> new T2<>(e.getKey(), e.getValue()))
            .collect(Collectors.toList());

        // Check that TEXT query actually works
        assertEqualsCollections(Collections.singleton(new T2<>(1, "john")), res);

        List<Object> expIdxs = Arrays.asList(
            Arrays.asList("cache", "STRING", "STRING__VAL_IDX", "\"_VAL\" ASC, \"_KEY\" ASC", "BTREE", false, false, 94416770, "cache", 94416770, "cache", 10),
            Arrays.asList("cache", "STRING", "__SCAN_", null, "SCAN", false, false, 94416770, "cache", 94416770, "cache", null),
            Arrays.asList("cache", "STRING", "_key_PK", "\"_KEY\" ASC", "BTREE", true, true, 94416770, "cache", 94416770, "cache", 5),
            Arrays.asList("cache", "STRING", "_key_PK_hash", "\"_KEY\" ASC", "HASH", false, true, 94416770, "cache", 94416770, "cache", null)
        );

        // It is expected that TEXT index is not present in the list
        checkIndexes(expIdxs::equals);
    }

    /** */
    private void checkIndexes(Predicate<List<List<?>>> checker) throws Exception {
        for (Ignite ign : G.allGrids()) {
            // Indexes view is empty on client nodes in 2.5
            if (ign.configuration().isClientMode())
                continue;

            assertTrue(GridTestUtils.waitForCondition(() -> {
                List<List<?>> indexes = execSql(ign, "SELECT * FROM IGNITE.INDEXES ORDER BY CACHE_NAME, INDEX_NAME");

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
