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

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Check starting cache in transaction.
 */
public class IgniteStartCacheInTransactionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String EXPECTED_MSG = "Cannot start/stop cache within lock or transaction";

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
