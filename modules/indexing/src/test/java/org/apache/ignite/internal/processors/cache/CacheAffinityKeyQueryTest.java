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

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests, that affinity key in binary metadata is registered correctly, when {@link QueryEntity} is configured.
 */
public class CacheAffinityKeyQueryTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String STATIC_CACHE_NAME = "staticCache";

    /** */
    private static final String DYNAMIC_CACHE_NAME = "dynamicCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (igniteInstanceName.equals("client"))
            cfg.setClientMode(true);

        cfg.setCacheConfiguration(cacheConfiguration(STATIC_CACHE_NAME, StaticKey.class, StaticValue.class));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityKeyRegisteredStaticCache() throws Exception {
        Ignite ignite = startGrid();


        assertEquals("affKey", getAffinityKey(ignite, StaticKey.class));
        assertEquals("affKey", getAffinityKey(ignite, StaticValue.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityKeyRegisteredDynamicCache() throws Exception {
        Ignite ignite = startGrid();

        ignite.createCache(cacheConfiguration(DYNAMIC_CACHE_NAME, DynamicKey.class, DynamicValue.class));

        assertEquals("affKey", getAffinityKey(ignite, DynamicKey.class));
        assertEquals("affKey", getAffinityKey(ignite, DynamicValue.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientFindsValueByAffinityKeyStaticCache() throws Exception {
        Ignite srv = startGrid();
        IgniteCache<StaticKey, StaticValue> cache = srv.cache(STATIC_CACHE_NAME);

        testClientFindsValueByAffinityKey(cache, new StaticKey(1), new StaticValue(2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientFindsValueByAffinityKeyDynamicCache() throws Exception {
        Ignite srv = startGrid();
        IgniteCache<DynamicKey, DynamicValue> cache =
            srv.createCache(cacheConfiguration(DYNAMIC_CACHE_NAME, DynamicKey.class, DynamicValue.class));

        testClientFindsValueByAffinityKey(cache, new DynamicKey(3), new DynamicValue(4));
    }

    /**
     * @param ignite Ignite instance.
     * @param keyCls Key class.
     * @return Name of affinity key field of the given class.
     */
    private <K> String getAffinityKey(Ignite ignite, Class<K> keyCls) {
        BinaryType binType = ignite.binary().type(keyCls);

        return binType.affinityKeyFieldName();
    }

    /**
     * @param cache Cache instance.
     * @param key Test key.
     * @param val Test value.
     * @throws Exception If failed.
     */
    private <K, V> void testClientFindsValueByAffinityKey(IgniteCache<K, V> cache, K key, V val) throws Exception {
        cache.put(key, val);

        assertTrue(cache.containsKey(key));

        Ignite client = startGrid("client");

        IgniteCache<K, V> clientCache = client.cache(cache.getName());

        assertTrue(clientCache.containsKey(key));
    }

    /**
     * @param name Cache name.
     * @param keyCls Key {@link Class}.
     * @param valCls Value {@link Class}.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Cache configuration
     */
    private static <K, V> CacheConfiguration<K, V> cacheConfiguration(String name, Class<K> keyCls, Class<V> valCls) {
        CacheConfiguration<K, V> cfg = new CacheConfiguration<>(name);
        cfg.setQueryEntities(Collections.singleton(new QueryEntity(keyCls, valCls)));
        return cfg;
    }

    /** */
    private static class StaticKey {
        /** */
        @AffinityKeyMapped
        private int affKey;

        /**
         * @param affKey Affinity key.
         */
        StaticKey(int affKey) {
            this.affKey = affKey;
        }
    }

    /** */
    private static class StaticValue {
        /** */
        @AffinityKeyMapped
        private int affKey;

        /**
         * @param affKey Affinity key.
         */
        StaticValue(int affKey) {
        }
    }

    /** */
    private static class DynamicKey {
        /** */
        @AffinityKeyMapped
        private int affKey;

        /**
         * @param affKey Affinity key.
         */
        DynamicKey(int affKey) {
            this.affKey = affKey;
        }
    }

    /** */
    private static class DynamicValue {
        /** */
        @AffinityKeyMapped
        private int affKey;

        /**
         * @param affKey Affinity key.
         */
        DynamicValue(int affKey) {
            this.affKey = affKey;
        }
    }
}
