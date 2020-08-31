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
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataRequestMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataResponseMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateAcceptedMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateProposedMessage;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests, that binary metadata is registered correctly during the start without extra request to grid.
 */
public class CacheRegisterMetadataLocallyTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String STATIC_CACHE_NAME = "staticCache";

    /** */
    private static final String DYNAMIC_CACHE_NAME = "dynamicCache";

    /** Holder of sent custom messages. */
    private final ConcurrentLinkedQueue<Object> customMessages = new ConcurrentLinkedQueue<>();

    /** Holder of sent communication messages. */
    private final ConcurrentLinkedQueue<Object> communicationMessages = new ConcurrentLinkedQueue<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi() {
            @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                if (msg instanceof CustomMessageWrapper) {
                    DiscoveryCustomMessage realMsg = ((CustomMessageWrapper)msg).delegate();

                    if (realMsg instanceof MetadataUpdateProposedMessage || realMsg instanceof MetadataUpdateAcceptedMessage)
                        customMessages.add(realMsg);
                }

                super.sendCustomEvent(msg);
            }
        });

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, Message msg,
                IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
                if (msg instanceof GridIoMessage)
                    communicationMessages.add(((GridIoMessage)msg).message());

                super.sendMessage(node, msg, ackC);
            }

            @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
                if (msg instanceof GridIoMessage)
                    communicationMessages.add(((GridIoMessage)msg).message());

                super.sendMessage(node, msg);
            }
        });

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCacheConfiguration(cacheConfiguration(STATIC_CACHE_NAME, StaticKey.class, StaticValue.class));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        customMessages.clear();
        communicationMessages.clear();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityKeyRegisteredStaticCache() throws Exception {
        Ignite ignite = startGrid(0);

        assertEquals("affKey", getAffinityKey(ignite, StaticKey.class));
        assertEquals("affKey", getAffinityKey(ignite, StaticValue.class));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityKeyRegisteredDynamicCache() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.createCache(cacheConfiguration(DYNAMIC_CACHE_NAME, DynamicKey.class, DynamicValue.class));

        assertEquals("affKey", getAffinityKey(ignite, DynamicKey.class));
        assertEquals("affKey", getAffinityKey(ignite, DynamicValue.class));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientFindsValueByAffinityKeyStaticCacheWithoutExtraRequest() throws Exception {
        Ignite srv = startGrid(0);
        IgniteCache<StaticKey, StaticValue> cache = srv.cache(STATIC_CACHE_NAME);

        testClientAndServerFindsValueByAffinityKey(cache, new StaticKey(1), new StaticValue(2));

        assertCustomMessages(2); //MetadataUpdateProposedMessage for update schema.
        assertCommunicationMessages();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientFindsValueByAffinityKeyDynamicCacheWithoutExtraRequest() throws Exception {
        Ignite srv = startGrid(0);
        IgniteCache<DynamicKey, DynamicValue> cache =
            srv.createCache(cacheConfiguration(DYNAMIC_CACHE_NAME, DynamicKey.class, DynamicValue.class));

        testClientAndServerFindsValueByAffinityKey(cache, new DynamicKey(3), new DynamicValue(4));

        //Expected only MetadataUpdateProposedMessage for update schema.
        assertCustomMessages(2);
        assertCommunicationMessages();
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
    private <K, V> void testClientAndServerFindsValueByAffinityKey(IgniteCache<K, V> cache, K key, V val) throws Exception {
        cache.put(key, val);

        assertTrue(cache.containsKey(key));

        Ignite client = startClientGrid("client");

        IgniteCache<K, V> clientCache = client.cache(cache.getName());

        assertTrue(clientCache.containsKey(key));

        Ignite server = startGrid(1);

        IgniteCache<K, V> serverCache = server.cache(cache.getName());

        assertTrue(serverCache.containsKey(key));
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

    /**
     * Expecting that "proposed binary metadata"( {@link org.apache.ignite.internal.processors.marshaller.MappingProposedMessage},
     * {@link org.apache.ignite.internal.processors.cache.binary.MetadataUpdateProposedMessage}) will be skipped because
     * it should be register locally during the start.
     *
     * @param expMsgCnt Count of expected messages.
     */
    private void assertCustomMessages(int expMsgCnt) {
        assertEquals(customMessages.toString(), expMsgCnt, customMessages.size());

        customMessages.forEach(cm -> assertTrue(cm.toString(), cm instanceof DynamicCacheChangeBatch || cm instanceof MetadataUpdateProposedMessage));
    }

    /**
     * Expecting that extra request to binary metadata( {@link MetadataRequestMessage}, {@link MetadataResponseMessage})
     * will be skipped because it should be register locally during the start.
     */
    private void assertCommunicationMessages() {
        communicationMessages.forEach(cm ->
            assertFalse(cm.getClass().getName(), cm instanceof MetadataRequestMessage || cm instanceof MetadataResponseMessage)
        );
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
        /** It doesn't make sense on value class. It it just for checking that value class also register correctly. */
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
        /** It doesn't make sense on value class. It it just for checking that value class also register correctly. */
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
