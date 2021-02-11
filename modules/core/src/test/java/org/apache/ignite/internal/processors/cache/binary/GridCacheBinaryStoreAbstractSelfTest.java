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

package org.apache.ignite.internal.processors.cache.binary;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import com.google.common.collect.ImmutableSet;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Tests for cache store with binary.
 */
public abstract class GridCacheBinaryStoreAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TestStore STORE = new TestStore();

    /** */
    protected static IgniteConfiguration cfg;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setNameMapper(new BinaryBasicNameMapper(false));
        bCfg.setIdMapper(new BinaryBasicIdMapper(false));

        bCfg.setClassNames(Arrays.asList(Key.class.getName(), Value.class.getName()));

        cfg.setBinaryConfiguration(bCfg);

        cfg.setMarshaller(new BinaryMarshaller());

        CacheConfiguration cacheCfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheStoreFactory(singletonFactory(STORE));
        cacheCfg.setStoreKeepBinary(keepBinaryInStore());
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        cfg.setCacheConfiguration(cacheCfg);

        GridCacheBinaryStoreAbstractSelfTest.cfg = cfg;

        return cfg;
    }

    /**
     * @return Keep binary in store flag.
     */
    protected abstract boolean keepBinaryInStore();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        STORE.map().clear();

        jcache().clear();

        assert jcache().size() == 0;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPut() throws Exception {
        jcache().put(new Key(1), new Value(1));

        checkMap(STORE.map(), 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAll() throws Exception {
        Map<Object, Object> map = new HashMap<>();

        for (int i = 1; i <= 3; i++)
            map.put(new Key(i), new Value(i));

        jcache().putAll(map);

        checkMap(STORE.map(), 1, 2, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoad() throws Exception {
        populateMap(STORE.map(), 1);

        Object val = jcache().get(new Key(1));

        assertTrue(String.valueOf(val), val instanceof Value);

        assertEquals(1, ((Value)val).index());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLoadAll() throws Exception {
        populateMap(STORE.map(), 1, 2, 3);

        Set<Object> keys = new HashSet<>();

        for (int i = 1; i <= 3; i++)
            keys.add(new Key(i));

        Map<Object, Object> res = jcache().getAll(keys);

        assertEquals(3, res.size());

        for (int i = 1; i <= 3; i++) {
            Object val = res.get(new Key(i));

            assertTrue(String.valueOf(val), val instanceof Value);

            assertEquals(i, ((Value)val).index());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemove() throws Exception {
        for (int i = 1; i <= 3; i++)
            jcache().put(new Key(i), new Value(i));

        jcache().remove(new Key(1));

        checkMap(STORE.map(), 2, 3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAll() throws Exception {
        for (int i = 1; i <= 3; i++)
            jcache().put(new Key(i), new Value(i));

        jcache().removeAll(ImmutableSet.of(new Key(1), new Key(2)));

        checkMap(STORE.map(), 3);
    }

    /**
     * @param map Map.
     * @param idxs Indexes.
     */
    protected abstract void populateMap(Map<Object, Object> map, int... idxs);

    /**
     * @param map Map.
     * @param idxs Indexes.
     */
    protected abstract void checkMap(Map<Object, Object> map, int... idxs);

    /**
     */
    protected static class Key {
        /** */
        private int idx;

        /**
         * @param idx Index.
         */
        public Key(int idx) {
            this.idx = idx;
        }

        /**
         * @return Index.
         */
        int index() {
            return idx;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            return idx == key.idx;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return idx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Key [idx=" + idx + ']';
        }
    }

    /**
     */
    protected static class Value {
        /** */
        private int idx;

        /**
         * @param idx Index.
         */
        public Value(int idx) {
            this.idx = idx;
        }

        /**
         * @return Index.
         */
        int index() {
            return idx;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Value [idx=" + idx + ']';
        }
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** */
        private final Map<Object, Object> map = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Nullable @Override public Object load(Object key) {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> e) {
            map.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            map.remove(key);
        }

        /**
         * @return Map.
         */
        Map<Object, Object> map() {
            return map;
        }
    }
}
