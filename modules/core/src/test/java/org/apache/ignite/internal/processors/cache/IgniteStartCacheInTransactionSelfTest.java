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

import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Check starting cache in transaction.
 */
@RunWith(JUnit4.class)
public class IgniteStartCacheInTransactionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String EXPECTED_MSG = "Cannot start/stop cache within lock or transaction.";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    public CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(cacheName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartCache() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        IgniteCache<String, String> cache = ignite.cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)){
            cache.put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.createCache("NEW_CACHE");

                    return null;
                }
            }, IgniteException.class, EXPECTED_MSG);

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartConfigurationCache() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        IgniteCache<String, String> cache = ignite.cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)){
            cache.put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.createCache(cacheConfiguration("NEW_CACHE"));

                    return null;
                }
            }, IgniteException.class, EXPECTED_MSG);

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStartConfigurationCacheWithNear() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        IgniteCache<String, String> cache = ignite.cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)){
            cache.put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.createCache(cacheConfiguration("NEW_CACHE"), new NearCacheConfiguration());

                    return null;
                }
            }, IgniteException.class, EXPECTED_MSG);

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetOrCreateCache() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        IgniteCache<String, String> cache = ignite.cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)){
            cache.put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.getOrCreateCache("NEW_CACHE");

                    return null;
                }
            }, IgniteException.class, EXPECTED_MSG);

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetOrCreateCacheConfiguration() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        IgniteCache<String, String> cache = ignite.cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)){
            cache.put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.getOrCreateCache(cacheConfiguration("NEW_CACHE"));

                    return null;
                }
            }, IgniteException.class, EXPECTED_MSG);

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopCache() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        IgniteCache<String, String> cache = ignite.cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)){
            cache.put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.destroyCache(DEFAULT_CACHE_NAME);

                    return null;
                }
            }, IgniteException.class, EXPECTED_MSG);

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLockCache() throws Exception {
        if (atomicityMode() != TRANSACTIONAL)
            return;

        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.ENTRY_LOCK);

        final Ignite ignite = grid(0);

        final String key = "key";

        Lock lock = ignite.cache(DEFAULT_CACHE_NAME).lock(key);

        lock.lock();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite.createCache("NEW_CACHE");

                return null;
            }
        }, IgniteException.class, EXPECTED_MSG);

        lock.unlock();
    }
}
