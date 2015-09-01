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
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Check starting cache in transaction.
 */
public class IgniteStartCacheInTransactionSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

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

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartCache() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)){
            ignite.cache(null).put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.createCache("NEW_CACHE");

                    return null;
                }
            }, IgniteException.class, "Cannot start/stop cache within transaction.");

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartConfigurationCache() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)){
            ignite.cache(null).put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.createCache(cacheConfiguration("NEW_CACHE"));

                    return null;
                }
            }, IgniteException.class, "Cannot start/stop cache within transaction.");

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartConfigurationCacheWithNear() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)){
            ignite.cache(null).put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.createCache(cacheConfiguration("NEW_CACHE"), new NearCacheConfiguration());

                    return null;
                }
            }, IgniteException.class, "Cannot start/stop cache within transaction.");

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetOrCreateCache() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)){
            ignite.cache(null).put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.getOrCreateCache("NEW_CACHE");

                    return null;
                }
            }, IgniteException.class, "Cannot start/stop cache within transaction.");

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetOrCreateCacheConfiguration() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)){
            ignite.cache(null).put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.getOrCreateCache(cacheConfiguration("NEW_CACHE"));

                    return null;
                }
            }, IgniteException.class, "Cannot start/stop cache within transaction.");

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopCache() throws Exception {
        final Ignite ignite = grid(0);

        final String key = "key";
        final String val = "val";

        try (Transaction tx = ignite.transactions().txStart(
            TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)){
            ignite.cache(null).put(key, val);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ignite.destroyCache(null);

                    return null;
                }
            }, IgniteException.class, "Cannot start/stop cache within transaction.");

            tx.commit();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockCache() throws Exception {
        if (atomicityMode() != TRANSACTIONAL)
            return;

        final Ignite ignite = grid(0);

        final String key = "key";

        Lock lock = ignite.cache(null).lock(key);

        lock.lock();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite.createCache("NEW_CACHE");

                return null;
            }
        }, IgniteException.class, "Cannot start/stop cache within lock.");

        lock.unlock();
    }
}