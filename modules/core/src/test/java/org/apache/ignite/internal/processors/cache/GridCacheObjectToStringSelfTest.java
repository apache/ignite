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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests that common cache objects' toString() methods do not lead to stack overflow.
 */
@RunWith(JUnit4.class)
public class GridCacheObjectToStringSelfTest extends GridCommonAbstractTest {
    /** Cache mode for test. */
    private CacheMode cacheMode;

    /** Cache eviction policy. */
    private EvictionPolicy evictionPlc;

    /** Near enabled flag. */
    private boolean nearEnabled;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(cacheMode);
        cacheCfg.setEvictionPolicy(evictionPlc);
        cacheCfg.setOnheapCacheEnabled(true);
        cacheCfg.setNearConfiguration(nearEnabled ? new NearCacheConfiguration() : null);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        evictionPlc = null;
    }

    /** @throws Exception If failed. */
    @Test
    public void testLocalCacheFifoEvictionPolicy() throws Exception {
        cacheMode = LOCAL;
        evictionPlc = new FifoEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testLocalCacheLruEvictionPolicy() throws Exception {
        cacheMode = LOCAL;
        evictionPlc = new LruEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testReplicatedCacheFifoEvictionPolicy() throws Exception {
        cacheMode = REPLICATED;
        evictionPlc = new FifoEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testReplicatedCacheLruEvictionPolicy() throws Exception {
        cacheMode = REPLICATED;
        evictionPlc = new LruEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testPartitionedCacheFifoEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = true;
        evictionPlc = new FifoEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testPartitionedCacheLruEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = true;
        evictionPlc = new LruEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testColocatedCacheFifoEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;
        evictionPlc = new FifoEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    @Test
    public void testColocatedCacheLruEvictionPolicy() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;
        evictionPlc = new LruEvictionPolicy();

        checkToString();
    }

    /** @throws Exception If failed. */
    private void checkToString() throws Exception {
        Ignite g = startGrid(0);

        try {
            IgniteCache<Object, Object> cache = g.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 10; i++)
                cache.put(i, i);

            for (int i = 0; i < 10; i++) {
                GridCacheEntryEx entry = ((IgniteKernal)g).context().cache().internalCache(DEFAULT_CACHE_NAME).peekEx(i);

                if (entry != null)
                    assertFalse("Entry is locked after implicit transaction commit: " + entry, entry.lockedByAny());
            }

            assertFalse(cache.toString().isEmpty());
            assertFalse(cache.iterator().toString().isEmpty());

            try (Transaction tx = g.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                assertEquals(1, cache.get(1));

                cache.put(2, 22);

                assertFalse(tx.toString().isEmpty());

                assertFalse(cache.toString().isEmpty());

                tx.commit();
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
