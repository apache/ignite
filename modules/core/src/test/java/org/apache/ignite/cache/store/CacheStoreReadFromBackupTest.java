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

package org.apache.ignite.cache.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CachePeekMode.BACKUP;
import static org.apache.ignite.cache.CachePeekMode.PRIMARY;

/**
 * Checks that once value is read from store, it will be loaded in
 * backups as well.
 */
public class CacheStoreReadFromBackupTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "cache";

    /** */
    private static final Map<Integer, String> storeMap = new ConcurrentHashMap<>();

    /** */
    private CacheMode cacheMode = REPLICATED;

    /** */
    private int backups;

    /** Near. */
    private boolean near;

    /** */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfig(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setCacheMode(cacheMode);
        ccfg.setBackups(backups);
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1));
        ccfg.setReadThrough(true);
        ccfg.setReadFromBackup(true);
        ccfg.setCacheStoreFactory(FactoryBuilder.factoryOf(TestStore.class));

        if (near)
            ccfg.setNearConfiguration(new NearCacheConfiguration());

        return ccfg;
    }

    /** */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName).setCacheConfiguration(cacheConfig(CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicated() throws Exception {
        cacheMode = REPLICATED;
        backups = 0;
        near = false;

        checkReadFromBackup();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitioned() throws Exception {
        cacheMode = PARTITIONED;
        backups = 1;
        near = false;

        checkReadFromBackup();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearReplicated() throws Exception {
        cacheMode = REPLICATED;
        backups = 0;
        near = true;

        checkReadFromBackup();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNearPartitioned() throws Exception {
        cacheMode = PARTITIONED;
        backups = 1;
        near = true;

        checkReadFromBackup();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkReadFromBackup() throws Exception {
        startGridsMultiThreaded(2, true);

        checkReadSingleFromBackup();
        checkReadAllFromBackup();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkReadSingleFromBackup() throws Exception {
        storeMap.put(1, "val-1");

        IgniteCache<Integer, String> cache0 = grid(0).cache(CACHE_NAME);
        IgniteCache<Integer, String> cache1 = grid(1).cache(CACHE_NAME);

        // Load value on primary and backup.
        assertNotNull(cache0.get(1));
        assertNotNull(cache1.get(1));

        if (cache0.localPeek(1, PRIMARY) != null)
            assertNotNull(cache1.localPeek(1, BACKUP));
        else {
            assertNotNull(cache0.localPeek(1, BACKUP));
            assertNotNull(cache1.localPeek(1, PRIMARY));
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkReadAllFromBackup() throws Exception {
        for (int i = 0; i < 100; i++)
            storeMap.put(i, String.valueOf(i));

        IgniteCache<Integer, String> cache0 = grid(0).cache(CACHE_NAME);
        IgniteCache<Integer, String> cache1 = grid(1).cache(CACHE_NAME);

        assertEquals(storeMap.size(), cache0.getAll(storeMap.keySet()).size());
        assertEquals(storeMap.size(), cache1.getAll(storeMap.keySet()).size());

        Affinity<Integer> aff = grid(0).affinity(CACHE_NAME);
        ClusterNode node0 = grid(0).cluster().localNode();

        for (Integer key : storeMap.keySet()) {
            if (aff.isPrimary(node0, key)) {
                assertNotNull(cache0.localPeek(key, PRIMARY));
                assertNotNull(cache1.localPeek(key, BACKUP));
            }
            else {
                assertNotNull(cache0.localPeek(key, BACKUP));
                assertNotNull(cache1.localPeek(key, PRIMARY));
            }
        }
    }

    /**
     *
     */
    public static class TestStore extends CacheStoreAdapter<Integer, String> {
        /** */
        public TestStore() {
        }

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, String> clo, Object... args) {
            for (Map.Entry<Integer, String> e : storeMap.entrySet())
                clo.apply(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public String load(Integer key) {
            return storeMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends String> entry) {
            storeMap.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SuspiciousMethodCalls")
        @Override public void delete(Object key) {
            storeMap.remove(key);
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            // No-op.
        }
    }
}
