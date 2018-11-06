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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
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
    private Collection<QueryIndex> indexes;

    /** */
    private Integer inlineSize;

    /** */
    private Boolean isPersistenceEnabled;

    /** */
    private String affKeyFieldName;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        assertNotNull(indexes);

        assertNotNull(inlineSize);

        assertNotNull(isPersistenceEnabled);

        for (QueryIndex index : indexes) {
            index.setInlineSize(inlineSize);
        }

        IgniteConfiguration igniteCfg = super.getConfiguration(igniteInstanceName);

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

        if (affKeyFieldName != null) {
            ccfg.setKeyConfiguration(new CacheKeyConfiguration()
                .setTypeName(Key.class.getTypeName())
                .setAffinityKeyFieldName(affKeyFieldName)
            );
        }

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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        indexes = null;

        inlineSize = null;

        isPersistenceEnabled = null;

        affKeyFieldName = null;

        super.afterTest();
    }

    /** */
    public void testNoIndexesNoPersistence() throws Exception {
        indexes = Collections.emptyList();

        isPersistenceEnabled = false;

        int[] inlineSizes = { 0, 10, 20, 50, 100 };

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGrid();

            populateCache();

            checkAll();

            stopGrid();
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

        isPersistenceEnabled = false;

        int[] inlineSizes = { 0, 10, 20, 50, 100 };

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGrid();

            populateCache();

            checkAll();

            stopGrid();
        }
    }

    /** */
    public void testDynamicIndexesNoPersistence() throws Exception {
        indexes = Collections.emptyList();

        isPersistenceEnabled = false;

        int[] inlineSizes = { 0, 10, 20, 50, 100 };

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGrid();

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

            stopGrid();
        }
    }

    /** */
    public void testNoIndexesWithPersistence() throws Exception {
        indexes = Collections.emptyList();

        isPersistenceEnabled = true;

        int[] inlineSizes = { 0, 10, 20, 50, 100 };

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGrid();

            grid().cluster().active(true);

            populateCache();

            checkAll();

            stopGrid();

            startGrid();

            grid().cluster().active(true);

            checkAll();

            stopGrid();

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

        int[] inlineSizes = { 0, 10, 20, 50, 100 };

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGrid();

            grid().cluster().active(true);

            populateCache();

            checkAll();

            stopGrid();

            startGrid();

            grid().cluster().active(true);

            checkAll();

            stopGrid();

            cleanPersistenceDir();
        }
    }

    /** */
    public void testDynamicIndexesWithPersistence() throws Exception {
        indexes = Collections.emptyList();

        isPersistenceEnabled = true;

        int[] inlineSizes = { 0, 10, 20, 50, 100 };

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGrid();

            grid().cluster().active(true);

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

            stopGrid();

            startGrid();

            grid().cluster().active(true);

            checkAll();

            stopGrid();

            cleanPersistenceDir();
        }
    }

    /** */
    public void testNoIndexesWithPersistenceIndexRebuild() throws Exception {
        indexes = Collections.emptyList();

        isPersistenceEnabled = true;

        int[] inlineSizes = { 0, 10, 20, 50, 100 };

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGrid();

            grid().cluster().active(true);

            populateCache();

            checkAll();

            Path idxPath = getIndexBinPath();

            // Shutdown gracefully to ensure there is a checkpoint with index.bin.
            // Otherwise index.bin rebuilding may not work.
            grid().cluster().active(false);

            stopGrid();

            assertTrue(U.delete(idxPath));

            startGrid();

            grid().cluster().active(true);

            grid().cache(DEFAULT_CACHE_NAME).indexReadyFuture().get();

            checkAll();

            stopGrid();

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

        int[] inlineSizes = { 0, 10, 20, 50, 100 };

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGrid();

            grid().cluster().active(true);

            populateCache();

            checkAll();

            Path idxPath = getIndexBinPath();

            // Shutdown gracefully to ensure there is a checkpoint with index.bin.
            // Otherwise index.bin rebuilding may not work.
            grid().cluster().active(false);

            stopGrid();

            assertTrue(U.delete(idxPath));

            startGrid();

            grid().cluster().active(true);

            grid().cache(DEFAULT_CACHE_NAME).indexReadyFuture().get();

            checkAll();

            stopGrid();

            cleanPersistenceDir();
        }
    }

    /** */
    public void testDynamicIndexesWithPersistenceIndexRebuild() throws Exception {
        indexes = Collections.emptyList();

        isPersistenceEnabled = true;

        int[] inlineSizes = { 0, 10, 20, 50, 100 };

        for (int i : inlineSizes) {
            log().info("Checking inlineSize=" + i);

            inlineSize = i;

            startGrid();

            grid().cluster().active(true);

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

            Path idxPath = getIndexBinPath();

            // Shutdown gracefully to ensure there is a checkpoint with index.bin.
            // Otherwise index.bin rebuilding may not work.
            grid().cluster().active(false);

            stopGrid();

            assertTrue(U.delete(idxPath));

            startGrid();

            grid().cluster().active(true);

            grid().cache(DEFAULT_CACHE_NAME).indexReadyFuture().get();

            checkAll();

            stopGrid();

            cleanPersistenceDir();
        }
    }

    /** */
    private void checkAll() {
        IgniteCache<Key, Val> cache = grid().cache(DEFAULT_CACHE_NAME);

        checkRemovePut(cache);

        checkSelectAll(cache);

        checkSelectStringEqual(cache);

        checkSelectLongEqual(cache);

        checkSelectStringRange(cache);

        checkSelectLongRange(cache);
    }

    /** */
    private void populateCache() {
        IgniteCache<Key, Val> cache = grid().cache(DEFAULT_CACHE_NAME);

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
            Key key = (Key)row.get(0);

            Val val = (Val)row.get(1);

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
            Key key = (Key)row.get(0);

            Val val = (Val)row.get(1);

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
            Key key = (Key)row.get(0);

            Val val = (Val)row.get(1);

            long i = key.keyLong;

            assertEquals(key(i), key);

            assertEquals(val(i), val);

            assertTrue(i >= RANGE_START && i < RANGE_END);
        }
    }

    /** Must be called when the grid is up. */
    private Path getIndexBinPath() {
        IgniteInternalCache<Object, Object> cachex = grid().cachex(DEFAULT_CACHE_NAME);

        assertNotNull(cachex);

        FilePageStoreManager pageStoreMgr = (FilePageStoreManager)cachex.context().shared().pageStore();

        assertNotNull(pageStoreMgr);

        File cacheWorkDir = pageStoreMgr.cacheWorkDir(cachex.configuration());

        return cacheWorkDir.toPath().resolve("index.bin");
    }

    /** */
    private void createDynamicIndexes(String... cols) {
        IgniteCache<Key, Val> cache = grid().cache(DEFAULT_CACHE_NAME);

        for (String col : cols) {
            cache.query(new SqlFieldsQuery(
                "create index on Val(" + col + ") INLINE_SIZE " + inlineSize
            ));
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

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            return keyLong == key.keyLong &&
                Objects.equals(keyStr, key.keyStr) &&
                Objects.equals(keyPojo, key.keyPojo);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(keyStr, keyLong, keyPojo);
        }

        /** {@inheritDoc} */
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

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Val val = (Val)o;

            return valLong == val.valLong &&
                Objects.equals(valStr, val.valStr) &&
                Objects.equals(valPojo, val.valPojo);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(valStr, valLong, valPojo);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Val.class, this);
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

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Pojo pojo = (Pojo)o;

            return pojoLong == pojo.pojoLong;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(pojoLong);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Pojo.class, this);
        }
    }
}
