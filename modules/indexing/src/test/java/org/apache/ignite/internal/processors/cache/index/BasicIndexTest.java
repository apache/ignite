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

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * A set of basic tests for caches with indexes.
 */
public class BasicIndexTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private Collection<QueryIndex> indexes = Collections.emptyList();

    /** */
    private Integer inlineSize;

    /** */
    private boolean isPersistenceEnabled;

    /** */
    private int gridCount = 1;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        assertNotNull(inlineSize);

        for (QueryIndex index : indexes)
            index.setInlineSize(inlineSize);

        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

        igniteCfg.setConsistentId(igniteInstanceName);

        igniteCfg.setDiscoverySpi(
            new TcpDiscoverySpi().setIpFinder(IP_FINDER)
        );

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("keyStr", String.class.getName());
        fields.put("keyLong", Long.class.getName());
        fields.put("keyPojo", Pojo.class.getName());
        fields.put("valStr", String.class.getName());
        fields.put("valLong", Long.class.getName());
        fields.put("valPojo", Pojo.class.getName());

        CacheConfiguration<Key, Val> ccfg = new CacheConfiguration<Key, Val>(DEFAULT_CACHE_NAME)
            .setQueryEntities(Collections.singleton(
                new QueryEntity()
                    .setKeyType(Key.class.getName())
                    .setValueType(Val.class.getName())
                    .setFields(fields)
                    .setIndexes(indexes)
            ))
            .setSqlIndexMaxInlineSize(inlineSize);

        igniteCfg.setCacheConfiguration(ccfg);

        if (isPersistenceEnabled) {
            igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)
                )
            );
        }

        return igniteCfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @return Grid count used in test.
     */
    protected int gridCount() {
        return gridCount;
    }

    /** */
    public void testNoIndexesNoPersistence() throws Exception {
        int[] inlineSizes = {0, 10, 20, 50, 100};

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            stopAllGrids();
        }
    }

    /** */
    public void testAllIndexesNoPersistence() throws Exception {
        indexes = Arrays.asList(
            new QueryIndex("keyStr"),
            new QueryIndex("keyLong"),
            new QueryIndex("keyPojo"),
            new QueryIndex("valStr"),
            new QueryIndex("valLong"),
            new QueryIndex("valPojo")
        );

        int[] inlineSizes = {0, 10, 20, 50, 100};

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            stopAllGrids();
        }
    }

    /** */
    public void testDynamicIndexesNoPersistence() throws Exception {
        int[] inlineSizes = {0, 10, 20, 50, 100};

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            createDynamicIndexes(
                "keyStr",
                "keyLong",
                "keyPojo",
                "valStr",
                "valLong",
                "valPojo"
            );

            checkAll();

            stopAllGrids();
        }
    }

    /** */
    public void testNoIndexesWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        int[] inlineSizes = {0, 10, 20, 50, 100};

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            stopAllGrids();

            startGridsMultiThreaded(gridCount());

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    public void testAllIndexesWithPersistence() throws Exception {
        indexes = Arrays.asList(
            new QueryIndex("keyStr"),
            new QueryIndex("keyLong"),
            new QueryIndex("keyPojo"),
            new QueryIndex("valStr"),
            new QueryIndex("valLong"),
            new QueryIndex("valPojo")
        );

        isPersistenceEnabled = true;

        int[] inlineSizes = {0, 10, 20, 50, 100};

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            stopAllGrids();

            startGridsMultiThreaded(gridCount());

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    public void testDynamicIndexesWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        int[] inlineSizes = {0, 10, 20, 50, 100};

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            createDynamicIndexes(
                "keyStr",
                "keyLong",
                "keyPojo",
                "valStr",
                "valLong",
                "valPojo"
            );

            checkAll();

            stopAllGrids();

            startGridsMultiThreaded(gridCount());

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    public void testDynamicIndexesDropWithPersistence() throws Exception {
        isPersistenceEnabled = true;

        int[] inlineSizes = {0, 10, 20, 50, 100};

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            String[] cols = {
                "keyStr",
                "keyLong",
                "keyPojo",
                "valStr",
                "valLong",
                "valPojo"
            };

            createDynamicIndexes(cols);

            checkAll();

            dropDynamicIndexes(cols);

            checkAll();

            stopAllGrids();

            startGridsMultiThreaded(gridCount());

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    public void testNoIndexesWithPersistenceIndexRebuild() throws Exception {
        isPersistenceEnabled = true;

        int[] inlineSizes = {0, 10, 20, 50, 100};

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            List<Path> idxPaths = getIndexBinPaths();

            // Shutdown gracefully to ensure there is a checkpoint with index.bin.
            // Otherwise index.bin rebuilding may not work.
            grid(0).cluster().active(false);

            stopAllGrids();

            idxPaths.forEach(idxPath -> assertTrue(U.delete(idxPath)));

            startGridsMultiThreaded(gridCount());

            grid(0).cache(DEFAULT_CACHE_NAME).indexReadyFuture().get();

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    public void testAllIndexesWithPersistenceIndexRebuild() throws Exception {
        indexes = Arrays.asList(
            new QueryIndex("keyStr"),
            new QueryIndex("keyLong"),
            new QueryIndex("keyPojo"),
            new QueryIndex("valStr"),
            new QueryIndex("valLong"),
            new QueryIndex("valPojo")
        );

        isPersistenceEnabled = true;

        int[] inlineSizes = {0, 10, 20, 50, 100};

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            checkAll();

            List<Path> idxPaths = getIndexBinPaths();

            // Shutdown gracefully to ensure there is a checkpoint with index.bin.
            // Otherwise index.bin rebuilding may not work.
            grid(0).cluster().active(false);

            stopAllGrids();

            idxPaths.forEach(idxPath -> assertTrue(U.delete(idxPath)));

            startGridsMultiThreaded(gridCount());

            grid(0).cache(DEFAULT_CACHE_NAME).indexReadyFuture().get();

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    public void testDynamicIndexesWithPersistenceIndexRebuild() throws Exception {
        isPersistenceEnabled = true;

        int[] inlineSizes = {0, 10, 20, 50, 100};

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGridsMultiThreaded(gridCount());

            populateCache();

            createDynamicIndexes(
                "keyStr",
                "keyLong",
                "keyPojo",
                "valStr",
                "valLong",
                "valPojo"
            );

            checkAll();

            List<Path> idxPaths = getIndexBinPaths();

            // Shutdown gracefully to ensure there is a checkpoint with index.bin.
            // Otherwise index.bin rebuilding may not work.
            grid(0).cluster().active(false);

            stopAllGrids();

            idxPaths.forEach(idxPath -> assertTrue(U.delete(idxPath)));

            startGridsMultiThreaded(gridCount());

            grid(0).cache(DEFAULT_CACHE_NAME).indexReadyFuture().get();

            checkAll();

            stopAllGrids();

            cleanPersistenceDir();
        }
    }

    /** */
    private void checkAll() {
        IgniteCache<Key, Val> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        checkRemovePut(cache);

        checkSelectAll(cache);

        checkSelectStringEqual(cache);

        checkSelectLongEqual(cache);

        checkSelectStringRange(cache);

        checkSelectLongRange(cache);
    }

    /** */
    private void populateCache() {
        IgniteCache<Key, Val> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        // Be paranoid and populate first even indexes in ascending order, then odd indexes in descending
        // to check that inserting in the middle works.

        for (int i = 0; i < 100; i += 2)
            cache.put(key(i), val(i));

        for (int i = 99; i > 0; i -= 2)
            cache.put(key(i), val(i));

        for (int i = 99; i > 0; i -= 2)
            assertEquals(val(i), cache.get(key(i)));
    }

    /** */
    private void checkRemovePut(IgniteCache<Key, Val> cache) {
        final int INT = 24;

        assertEquals(val(INT), cache.get(key(INT)));

        cache.remove(key(INT));

        assertNull(cache.get(key(INT)));

        cache.put(key(INT), val(INT));

        assertEquals(val(INT), cache.get(key(INT)));
    }

    /** */
    private void checkSelectAll(IgniteCache<Key, Val> cache) {
        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val")).getAll();

        assertEquals(100, data.size());

        for (List<?> row : data) {
            Key key = (Key) row.get(0);

            Val val = (Val) row.get(1);

            long i = key.keyLong;

            assertEquals(key(i), key);

            assertEquals(val(i), val);
        }
    }

    /** */
    private void checkSelectStringEqual(IgniteCache<Key, Val> cache) {
        final String STR = "foo011";

        final long LONG = 11;

        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val where keyStr = ?")
            .setArgs(STR))
            .getAll();

        assertEquals(1, data.size());

        List<?> row = data.get(0);

        assertEquals(key(LONG), row.get(0));

        assertEquals(val(LONG), row.get(1));
    }

    /** */
    private void checkSelectLongEqual(IgniteCache<Key, Val> cache) {
        final long LONG = 42;

        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val where valLong = ?")
            .setArgs(LONG))
            .getAll();

        assertEquals(1, data.size());

        List<?> row = data.get(0);

        assertEquals(key(LONG), row.get(0));

        assertEquals(val(LONG), row.get(1));
    }

    /** */
    private void checkSelectStringRange(IgniteCache<Key, Val> cache) {
        final String PREFIX = "foo06";

        List<List<?>> data = cache.query(new SqlFieldsQuery("select _key, _val from Val where keyStr like ?")
            .setArgs(PREFIX + "%"))
            .getAll();

        assertEquals(10, data.size());

        for (List<?> row : data) {
            Key key = (Key) row.get(0);

            Val val = (Val) row.get(1);

            long i = key.keyLong;

            assertEquals(key(i), key);

            assertEquals(val(i), val);

            assertTrue(key.keyStr.startsWith(PREFIX));
        }
    }

    /** */
    private void checkSelectLongRange(IgniteCache<Key, Val> cache) {
        final long RANGE_START = 70;

        final long RANGE_END = 80;

        List<List<?>> data = cache.query(
            new SqlFieldsQuery("select _key, _val from Val where valLong >= ? and valLong < ?")
                .setArgs(RANGE_START, RANGE_END))
            .getAll();

        assertEquals(10, data.size());

        for (List<?> row : data) {
            Key key = (Key) row.get(0);

            Val val = (Val) row.get(1);

            long i = key.keyLong;

            assertEquals(key(i), key);

            assertEquals(val(i), val);

            assertTrue(i >= RANGE_START && i < RANGE_END);
        }
    }

    /**
     * Must be called when the grid is up.
     */
    private List<Path> getIndexBinPaths() {
        return G.allGrids().stream()
            .map(grid -> (IgniteEx) grid)
            .map(grid -> {
                IgniteInternalCache<Object, Object> cachex = grid.cachex(DEFAULT_CACHE_NAME);

                assertNotNull(cachex);

                FilePageStoreManager pageStoreMgr = (FilePageStoreManager) cachex.context().shared().pageStore();

                assertNotNull(pageStoreMgr);

                File cacheWorkDir = pageStoreMgr.cacheWorkDir(cachex.configuration());

                return cacheWorkDir.toPath().resolve("index.bin");
            })
            .collect(Collectors.toList());
    }

    /** */
    private void createDynamicIndexes(String... cols) {
        IgniteCache<Key, Val> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (String col : cols) {
            String indexName = col + "_idx";
            String schemaName = DEFAULT_CACHE_NAME;

            cache.query(new SqlFieldsQuery(
                String.format("create index %s on \"%s\".Val(%s) INLINE_SIZE %s;", indexName, schemaName, col, inlineSize)
            )).getAll();
        }

        cache.indexReadyFuture().get();
    }

    /** */
    private void dropDynamicIndexes(String... cols) {
        IgniteCache<Key, Val> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (String col : cols) {
            String indexName = col + "_idx";

            cache.query(new SqlFieldsQuery(
                String.format("drop index %s;", indexName)
            )).getAll();
        }

        cache.indexReadyFuture().get();
    }

    /** */
    private static Key key(long i) {
        return new Key(String.format("foo%03d", i), i, new Pojo(i));
    }

    /** */
    private static Val val(long i) {
        return new Val(String.format("bar%03d", i), i, new Pojo(i));
    }

    /** */
    private static class Key {
        /** */
        private String keyStr;

        /** */
        private long keyLong;

        /** */
        private Pojo keyPojo;

        /** */
        private Key(String str, long aLong, Pojo pojo) {
            keyStr = str;
            keyLong = aLong;
            keyPojo = pojo;
        }

        /**
         * {@inheritDoc}
         */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key) o;

            return keyLong == key.keyLong &&
                Objects.equals(keyStr, key.keyStr) &&
                Objects.equals(keyPojo, key.keyPojo);
        }

        /**
         * {@inheritDoc}
         */
        @Override public int hashCode() {
            return Objects.hash(keyStr, keyLong, keyPojo);
        }

        /**
         * {@inheritDoc}
         */
        @Override public String toString() {
            return S.toString(Key.class, this);
        }
    }

    /** */
    private static class Val {
        /** */
        private String valStr;

        /** */
        private long valLong;

        /** */
        private Pojo valPojo;

        /** */
        private Val(String str, long aLong, Pojo pojo) {
            valStr = str;
            valLong = aLong;
            valPojo = pojo;
        }

        /**
         * {@inheritDoc}
         */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Val val = (Val) o;

            return valLong == val.valLong &&
                Objects.equals(valStr, val.valStr) &&
                Objects.equals(valPojo, val.valPojo);
        }

        /**
         * {@inheritDoc}
         */
        @Override public int hashCode() {
            return Objects.hash(valStr, valLong, valPojo);
        }

        /**
         * {@inheritDoc}
         */
        @Override public String toString() {
            return S.toString(Val.class, this);
        }
    }

    /** */
    private static class Val2 {
        /** */
        private String valStr;

        /** */
        private long valLong;

        /** */
        private Pojo valPojo;

        /** */
        private Val2(String str, long aLong, Pojo pojo) {
            valStr = str;
            valLong = aLong;
            valPojo = pojo;
        }

        /**
         * {@inheritDoc}
         */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Val2 val = (Val2) o;

            return valLong == val.valLong &&
                Objects.equals(valStr, val.valStr) &&
                Objects.equals(valPojo, val.valPojo);
        }

        /**
         * {@inheritDoc}
         */
        @Override public int hashCode() {
            return Objects.hash(valStr, valLong, valPojo);
        }

        /**
         * {@inheritDoc}
         */
        @Override public String toString() {
            return S.toString(Val2.class, this);
        }
    }

    /** */
    private static class Pojo {
        /** */
        private long pojoLong;

        /** */
        private Pojo(long pojoLong) {
            this.pojoLong = pojoLong;
        }

        /**
         * {@inheritDoc}
         */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Pojo pojo = (Pojo) o;

            return pojoLong == pojo.pojoLong;
        }

        /**
         * {@inheritDoc}
         */
        @Override public int hashCode() {
            return Objects.hash(pojoLong);
        }

        /**
         * {@inheritDoc}
         */
        @Override public String toString() {
            return S.toString(Pojo.class, this);
        }
    }
}
