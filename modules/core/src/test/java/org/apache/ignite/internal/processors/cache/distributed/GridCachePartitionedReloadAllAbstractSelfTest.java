/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.integration.CompletionListenerFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Check reloadAll() on partitioned cache.
 */
@RunWith(JUnit4.class)
public abstract class GridCachePartitionedReloadAllAbstractSelfTest extends GridCommonAbstractTest {
    /** Amount of nodes in the grid. */
    private static final int GRID_CNT = 4;

    /** Amount of backups in partitioned cache. */
    private static final int BACKUP_CNT = 1;

    /** Map where dummy cache store values are stored. */
    private static final Map<Integer, String> map = new ConcurrentHashMap<>();

    /** Collection of caches, one per grid node. */
    private static List<IgniteCache<Integer, String>> caches;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc = defaultCacheConfiguration();

        if (!nearEnabled())
            cc.setNearConfiguration(null);

        cc.setCacheMode(cacheMode());

        cc.setAtomicityMode(atomicityMode());

        cc.setBackups(BACKUP_CNT);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        CacheStore store = cacheStore();

        if (store != null) {
            cc.setCacheStoreFactory(singletonFactory(store));
            cc.setReadThrough(true);
            cc.setWriteThrough(true);
            cc.setLoadPreviousValue(true);
        }
        else
            cc.setCacheStoreFactory(null);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Cache mode.
     */
    protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.TRANSACTIONAL;
    }

    /**
     * @return {@code True} if near cache is enabled.
     */
    protected abstract boolean nearEnabled();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        caches = new ArrayList<>(GRID_CNT);

        for (int i = 0; i < GRID_CNT; i++)
            caches.add(startGrid(i).<Integer, String>cache(DEFAULT_CACHE_NAME));

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        map.clear();
    }

    /**
     * Create new cache store.
     *
     * @return Write through storage emulator.
     */
    protected CacheStore<?, ?> cacheStore() {
        return new CacheStoreAdapter<Integer, String>() {
            @IgniteInstanceResource
            private Ignite g;

            @Override public void loadCache(IgniteBiInClosure<Integer, String> c,
                Object... args) {
                X.println("Loading all on: " + caches.indexOf(((IgniteKernal)g).<Integer, String>getCache(DEFAULT_CACHE_NAME)));

                for (Map.Entry<Integer, String> e : map.entrySet())
                    c.apply(e.getKey(), e.getValue());
            }

            @Override public String load(Integer key) {
                X.println("Loading on: " + caches.indexOf(((IgniteKernal)g)
                    .<Integer, String>getCache(DEFAULT_CACHE_NAME)) + " key=" + key);

                return map.get(key);
            }

            @Override public void write(javax.cache.Cache.Entry<? extends Integer, ? extends String> e) {
                fail("Should not be called within the test.");
            }

            @Override public void delete(Object key) {
                fail("Should not be called within the test.");
            }
        };
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testReloadAll() throws Exception {
        // Fill caches with values.
        for (IgniteCache<Integer, String> cache : caches) {
            Iterable<Integer> keys = primaryKeys(cache, 100);

            info("Values [cache=" + caches.indexOf(cache) + ", size=" + F.size(keys.iterator()) +  ", keys=" + keys + "]");

            for (Integer key : keys)
                map.put(key, "val" + key);
        }

        CompletionListenerFuture fut = new CompletionListenerFuture();

        caches.get(0).loadAll(map.keySet(), false, fut);

        fut.get();

        Affinity aff = ignite(0).affinity(DEFAULT_CACHE_NAME);

        for (IgniteCache<Integer, String> cache : caches) {
            for (Integer key : map.keySet()) {
                if (aff.isPrimaryOrBackup(grid(caches.indexOf(cache)).localNode(), key))
                    assertEquals(map.get(key), cache.localPeek(key));
                else
                    assertNull(cache.localPeek(key));
            }
        }
    }
}
