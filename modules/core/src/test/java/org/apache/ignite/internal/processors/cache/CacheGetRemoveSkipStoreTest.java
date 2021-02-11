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

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CacheGetRemoveSkipStoreTest extends GridCommonAbstractTest {
    /** */
    public static final String TEST_CACHE = "testCache";

    /** Number of nodes for the test. */
    public static final int GRID_CNT = 3;

    /** Read semaphore to delay read-through. */
    private static volatile Semaphore readSemaphore;

    /**
     * Creates cache configuration with the given atomicity mode and number of backups.
     *
     * @param atomicity Atomicity mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> configuration(
        CacheAtomicityMode atomicity,
        int backups
    ) {
        return new CacheConfiguration<Integer, Integer>()
            .setCacheMode(CacheMode.PARTITIONED)
            .setName(TEST_CACHE)
            .setAtomicityMode(atomicity)
            .setBackups(backups)
            .setCacheStoreFactory(TestCacheStore::new)
            .setReadThrough(true)
            .setWriteThrough(false);
    }

    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        readSemaphore = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);

        startClientGrid("client");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTransactionalNoBackups() throws Exception {
        checkNoNullReads(grid("client"), configuration(CacheAtomicityMode.TRANSACTIONAL, 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTransactionalOneBackup() throws Exception {
        checkNoNullReads(grid("client"), configuration(CacheAtomicityMode.TRANSACTIONAL, 1));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAtomicNoBackups() throws Exception {
        checkNoNullReads(grid("client"), configuration(CacheAtomicityMode.ATOMIC, 0));
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testAtomicOneBackup() throws Exception {
        checkNoNullReads(grid("client"), configuration(CacheAtomicityMode.ATOMIC, 1));
    }

    /**
     * @throws Exception if failed.
     */
    private void checkNoNullReads(Ignite client, CacheConfiguration<Integer, Integer> ccfg) throws Exception {
        client.getOrCreateCache(ccfg);

        try {
            checkNoNullReads(client.cache(ccfg.getName()), 0);
            checkNoNullReads(grid(0).cache(ccfg.getName()), primaryKey(grid(0).cache(ccfg.getName())));

            if (ccfg.getBackups() > 0)
                checkNoNullReads(grid(0).cache(ccfg.getName()), backupKey(grid(0).cache(ccfg.getName())));
        }
        finally {
            client.destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cache Cache to check.
     * @param key Key to check.
     * @throws Exception If failed.
     */
    private void checkNoNullReads(IgniteCache<Integer, Integer> cache, Integer key) throws Exception {
        assertNotNull(cache.get(key));

        AtomicReference<String> failure = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean(false);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            while (!stop.get()) {
                Object res = cache.get(key);

                if (res == null)
                    failure.compareAndSet(null, "Failed in thread: " + Thread.currentThread().getName());
            }
        });

        for (int i = 0; i < 100; i++)
            cache.remove(key);

        U.sleep(100);

        stop.set(true);
        fut.get();

        assertNotNull(cache.get(key));

        assertNull(failure.get());
    }

    /**
     */
    @Test
    public void testRemoveIsAppliedTransactionalNoBackups() {
        checkRemoveIsApplied(grid("client"), configuration(CacheAtomicityMode.TRANSACTIONAL, 0));
    }

    /**
     */
    @Test
    public void testRemoveIsAppliedTransactionalOneBackups() {
        checkRemoveIsApplied(grid("client"), configuration(CacheAtomicityMode.TRANSACTIONAL, 1));
    }

    /**
     */
    @Test
    public void testRemoveIsAppliedAtomicNoBackups() {
        checkRemoveIsApplied(grid("client"), configuration(CacheAtomicityMode.ATOMIC, 0));
    }

    /**
     */
    @Test
    public void testRemoveIsAppliedAtomicOneBackups() {
        checkRemoveIsApplied(grid("client"), configuration(CacheAtomicityMode.ATOMIC, 1));
    }

    /**
     * @param client Client to test.
     * @param ccfg Cache configuration
     */
    public void checkRemoveIsApplied(Ignite client, CacheConfiguration<Integer, Integer> ccfg) {
        // Allow first read.
        readSemaphore = new Semaphore(1);

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(ccfg);

        try {
            Integer key = 1;

            assertNotNull(cache.get(key));

            Ignite primary = grid(client.affinity(ccfg.getName()).mapKeyToNode(key));

            assertNotNull(primary.cache(ccfg.getName()).localPeek(key));

            // Read-through will be blocked on semaphore.
            IgniteFuture<Integer> getFut = cache.getAsync(key);

            cache.remove(key);

            assertNull(primary.cache(ccfg.getName()).localPeek(key)); // Check that remove actually takes place.

            readSemaphore.release(2);

            assertNotNull(getFut.get());
            assertNotNull(cache.get(key));
            assertNotNull(primary.cache(ccfg.getName()).localPeek(key));
        }
        finally {
            client.destroyCache(ccfg.getName());
        }
    }

    /**
     * Dummy cache store which delays key load and loads a predefined value.
     */
    public static class TestCacheStore extends CacheStoreAdapter<Integer, Integer> {
        /** */
        static final Integer CONSTANT_VALUE = -1;

        /** {@inheritDoc} */
        @Override public Integer load(Integer s) throws CacheLoaderException {
            try {
                if (readSemaphore != null)
                    readSemaphore.acquire();
                else
                    U.sleep(1000);
            }
            catch (Exception e) {
                throw new CacheLoaderException(e);
            }

            return CONSTANT_VALUE;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object o) throws CacheWriterException {
            // No-op.
        }
    }
}
