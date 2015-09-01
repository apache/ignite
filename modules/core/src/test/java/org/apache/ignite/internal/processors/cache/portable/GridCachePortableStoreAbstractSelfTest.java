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

package org.apache.ignite.internal.processors.cache.portable;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * Tests for cache store with portables.
 */
public abstract class GridCachePortableStoreAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final TestStore STORE = new TestStore();

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(Key.class.getName(), Value.class.getName()));

        cfg.setMarshaller(marsh);

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setCacheStoreFactory(singletonFactory(STORE));
        cacheCfg.setKeepPortableInStore(keepPortableInStore());
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @return Keep portables in store flag.
     */
    protected abstract boolean keepPortableInStore();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid();
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
    public void testPut() throws Exception {
        jcache().put(new Key(1), new Value(1));

        checkMap(STORE.map(), 1);
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testLoad() throws Exception {
        populateMap(STORE.map(), 1);

        Object val = jcache().get(new Key(1));

        assertTrue(String.valueOf(val), val instanceof Value);

        assertEquals(1, ((Value)val).index());
    }

    /**
     * @throws Exception If failed.
     */
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
    public void testRemove() throws Exception {
        for (int i = 1; i <= 3; i++)
            jcache().put(new Key(i), new Value(i));

        jcache().remove(new Key(1));

        checkMap(STORE.map(), 2, 3);
    }

    /**
     * @throws Exception If failed.
     */
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
        private final Map<Object, Object> map = new ConcurrentHashMap8<>();

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