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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;

/**
 * Test that store is called correctly on puts.
 */
public class GridCachePartitionedStorePutSelfTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicInteger CNT = new AtomicInteger(0);

    /** */
    private static AtomicInteger loads;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration());
        cfg.setUserAttributes(F.asMap(IDX_ATTR, CNT.getAndIncrement()));

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setCacheStoreFactory(singletonFactory(new TestStore()));
        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);
        cfg.setLoadPreviousValue(true);
        cfg.setAffinity(new GridCacheModuloAffinityFunction(3, 1));
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        loads = new AtomicInteger();

        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testPutShouldNotTriggerLoad() {
        checkPut(0);

        assertEquals(0, loads.get());

        checkPut(1);

        assertEquals(0, loads.get());

        checkPut(2);

        assertEquals(0, loads.get());
    }

    /**     */
    public void checkPut(int idx) {
        IgniteCache<Object, Object> cache = grid(idx).cache(DEFAULT_CACHE_NAME);

        cache.put(0, 1);

        try (Transaction tx = grid(idx).transactions().txStart()) {
            cache.put(1, 1);
            cache.put(2, 2);

            tx.commit();
        }

        assertEquals(0, loads.get());
    }

    /**
     * Test store.
     */
    private static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            loads.incrementAndGet();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Object, ? extends Object> e) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op
        }
    }
}
