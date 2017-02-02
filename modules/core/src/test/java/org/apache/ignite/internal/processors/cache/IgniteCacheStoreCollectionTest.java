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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
public class IgniteCacheStoreCollectionTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration<Object, Object> ccfg1 = new CacheConfiguration<>();
        ccfg1.setName("cache1");
        ccfg1.setAtomicityMode(ATOMIC);
        ccfg1.setWriteSynchronizationMode(FULL_SYNC);

        CacheConfiguration<Object, Object> ccfg2 = new CacheConfiguration<>();
        ccfg2.setName("cache2");
        ccfg2.setAtomicityMode(TRANSACTIONAL);
        ccfg2.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg1, ccfg2);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreMap() throws Exception {
        IgniteCache<Object, Object> cache1 = ignite(0).cache("cache1");
        IgniteCache<Object, Object> cache2 = ignite(0).cache("cache2");

        checkStoreMap(cache1);
        checkStoreMap(cache2);
    }

    /**
     * @param cache Cache.
     */
    private void checkStoreMap(IgniteCache<Object, Object> cache) {
        cache.put(1, new MyMap());
        cache.put(2, new MyMap());

        MyMap map = (MyMap)cache.get(1);

        assertNotNull(map);

        Map<Integer, MyMap> vals = (Map)cache.getAll(F.asSet(1, 2));

        assertEquals(2, vals.size());
        assertTrue("Unexpected value: " + vals.get(1), vals.get(1) instanceof MyMap);
        assertTrue("Unexpected value: " + vals.get(2), vals.get(2) instanceof MyMap);
    }

    /**
     *
     */
    public static class MyMap implements Map {
        /** {@inheritDoc} */
        @Override public int size() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean containsKey(Object key) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean containsValue(Object val) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Object get(Object key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Object put(Object key, Object val) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Object remove(Object key) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void putAll(Map m) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void clear() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Set keySet() {
            return Collections.emptySet();
        }

        /** {@inheritDoc} */
        @Override public Collection values() {
            return Collections.emptySet();
        }

        /** {@inheritDoc} */
        @Override public Set<Entry> entrySet() {
            return Collections.emptySet();
        }
    }
}
