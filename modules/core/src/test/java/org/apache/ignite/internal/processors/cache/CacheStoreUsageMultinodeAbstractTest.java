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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.store.CacheLocalStore;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_GRID_NAME;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public abstract class CacheStoreUsageMultinodeAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected boolean client;

    /** */
    protected boolean cache;

    /** */
    protected boolean cacheStore;

    /** */
    protected boolean locStore;

    /** */
    protected boolean writeBehind;

    /** */
    protected boolean nearCache;

    /** */
    protected static Map<String, List<Cache.Entry<?, ?>>> writeMap;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(client);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (cache)
            cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setAtomicWriteOrderMode(CacheAtomicWriteOrderMode.PRIMARY);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheStore) {
            if (writeBehind) {
                ccfg.setWriteBehindEnabled(true);
                ccfg.setWriteBehindFlushFrequency(100);
            }

            ccfg.setWriteThrough(true);

            ccfg.setCacheStoreFactory(locStore ? new TestLocalStoreFactory() : new TestStoreFactory());
        }

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration());

        return ccfg;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        writeMap = new HashMap<>();
    }

    /**
     * @param clientStore {@code True} if store configured on client node.
     * @throws Exception If failed.
     */
    protected void checkStoreUpdate(boolean clientStore) throws Exception {
        Ignite client = grid(3);

        assertTrue(client.configuration().isClientMode());

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache0 = ignite(0).cache(null);
        IgniteCache<Object, Object> cache1 = ignite(1).cache(null);
        IgniteCache<Object, Object> clientCache = client.cache(null);

        assertTrue(((IgniteCacheProxy)cache0).context().store().configured());
        assertEquals(clientStore, ((IgniteCacheProxy) clientCache).context().store().configured());

        List<TransactionConcurrency> tcList = new ArrayList<>();

        tcList.add(null);

        if (atomicityMode() == TRANSACTIONAL) {
            tcList.add(TransactionConcurrency.OPTIMISTIC);
            tcList.add(TransactionConcurrency.PESSIMISTIC);
        }

        log.info("Start test [atomicityMode=" + atomicityMode() +
            ", locStore=" + locStore +
            ", writeBehind=" + writeBehind +
            ", nearCache=" + nearCache +
            ", clientStore=" + clientStore + ']');

        for (TransactionConcurrency tc : tcList) {
            testStoreUpdate(cache0, primaryKey(cache0), tc);

            testStoreUpdate(cache0, backupKey(cache0), tc);

            testStoreUpdate(cache0, nearKey(cache0), tc);

            testStoreUpdate(cache0, primaryKey(cache1), tc);

            testStoreUpdate(clientCache, primaryKey(cache0), tc);

            testStoreUpdate(clientCache, primaryKey(cache1), tc);
        }
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param tc Transaction concurrency mode.
     * @throws Exception If failed.
     */
    protected void testStoreUpdate(IgniteCache<Object, Object> cache,
       Object key,
       @Nullable TransactionConcurrency tc)
        throws Exception
    {
        boolean storeOnPrimary = atomicityMode() == ATOMIC || locStore || writeBehind;

        assertTrue(writeMap.isEmpty());

        Ignite ignite = cache.unwrap(Ignite.class);

        Affinity<Object> obj = ignite.affinity(cache.getName());

        ClusterNode node = obj.mapKeyToNode(key);

        assertNotNull(node);

        String expNode = storeOnPrimary ? (String)node.attribute(ATTR_GRID_NAME) : ignite.name();

        assertNotNull(expNode);

        log.info("Put [node=" + ignite.name() +
            ", key=" + key +
            ", primary=" + node.attribute(ATTR_GRID_NAME) +
            ", tx=" + tc +
            ", nearCache=" + (cache.getConfiguration(CacheConfiguration.class).getNearConfiguration() != null) +
            ", storeOnPrimary=" + storeOnPrimary + ']');

        Transaction tx = tc != null ? ignite.transactions().txStart(tc, REPEATABLE_READ) : null;

        try {
            cache.put(key, key);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override
            public boolean apply() {
                return writeMap.size() > 0;
            }
        }, 1000);

        assertTrue("Store is not updated", wait);

        assertEquals("Write on wrong node: " + writeMap, 1, writeMap.size());

        assertEquals(expNode, writeMap.keySet().iterator().next());

        writeMap.clear();
    }

    /**
     *
     */
    public static class TestStoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new TestStore();
        }
    }

    /**
     *
     */
    public static class TestLocalStoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new TestLocalStore();
        }
    }

    /**
     *
     */
    public static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @SuppressWarnings("SynchronizeOnNonFinalField")
        @Override public void write(Cache.Entry<?, ?> entry) {
            synchronized (writeMap) {
                ignite.log().info("Write [node=" + ignite.name() + ", entry=" + entry + ']');

                String name = ignite.name();

                List<Cache.Entry<?, ?>> list = writeMap.get(name);

                if (list == null) {
                    list = new ArrayList<>();

                    writeMap.put(name, list);
                }

                list.add(entry);
            }
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     *
     */
    @CacheLocalStore
    public static class TestLocalStore extends TestStore {
        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }
}
