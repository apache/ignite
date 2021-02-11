/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test what interruptions of writing threads do not affect PDS.
 */
public class IgnitePdsThreadInterruptionTest extends GridCommonAbstractTest {
    /** */
    public static final int THREADS_CNT = 100;

    /** */
    private static final int VAL_LEN = 8192;

    /** */
    private static final byte[] PAYLOAD = new byte[VAL_LEN];

    /** */
    private volatile boolean stop;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setWalFsyncDelayNanos(0)
            .setFileIOFactory(new AsyncFileIOFactory())
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setInitialSize(10L * 1024L * 1024L)
                    .setMaxSize(10L * 1024L * 1024L)
            ));

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 1))
        );

        return cfg;
    }

    @Before
    public void before() throws Exception {
        cleanPersistenceDir();
    }

    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests interruptions on read.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInterruptsOnRead() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        int maxKey = 10_000;

        Set<Integer> keysToCheck = new HashSet<>();

        Thread[] workers = new Thread[THREADS_CNT];

        // Load data.
        try (IgniteDataStreamer<Integer, byte[]> st = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < maxKey; i++) {
                keysToCheck.add(i);

                st.addData(i, PAYLOAD);
            }
        }

        IgniteCache<Integer, byte[]> cache = ignite.cache(DEFAULT_CACHE_NAME);

        AtomicReference<Throwable> fail = new AtomicReference<>();

        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Thread(() -> cache.get(ThreadLocalRandom.current().nextInt(maxKey / 5)));

            workers[i].setName("reader-" + i);

            workers[i].setUncaughtExceptionHandler((t, e) -> {
                // We can get IgniteInterruptedException on GridCacheAdapter.asyncOpsSem if thread was interrupted
                // before asyncOpsSem.acquire().
                if (!X.hasCause(e,
                    "Failed to wait for asynchronous operation permit",
                    IgniteInterruptedException.class)) {
                    fail.compareAndSet(null, e);
                }
            });
        }

        for (Thread worker : workers)
            worker.start();

        // Interrupts should not affect reads.
        for (int i = 0;i < workers.length / 2; i++)
            workers[i].interrupt();

        U.sleep(3_000);

        for (Thread worker : workers)
            worker.join();

        Throwable t = fail.get();

        assertNull(t);

        int verifiedKeys = 0;

        // Get all keys.
        Map<Integer, byte[]> res = cache.getAll(keysToCheck);

        Assert.assertEquals(maxKey, keysToCheck.size());
        Assert.assertEquals(maxKey, res.size());

        // Post check.
        for (Integer key: keysToCheck) {
            byte[] val = res.get(key);

            assertNotNull(val);
            assertEquals("Illegal length", VAL_LEN, val.length);

            verifiedKeys++;
        }

        Assert.assertEquals(maxKey, verifiedKeys);

        log.info("Verified keys: " + verifiedKeys);
    }

    /**
     * Tests interruptions on WAL write.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInterruptsOnWALWrite() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        int maxKey = 100_000;

        Set<Integer> keysToCheck = new GridConcurrentHashSet<>();

        Thread[] workers = new Thread[THREADS_CNT];

        AtomicReference<Throwable> fail = new AtomicReference<>();

        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Thread(() -> {
                IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

                while (!stop) {
                    int key = ThreadLocalRandom.current().nextInt(maxKey);

                    cache.put(key, PAYLOAD);

                    keysToCheck.add(key);
                }
            });

            workers[i].setName("writer-" + i);

            workers[i].setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override public void uncaughtException(Thread t, Throwable e) {
                    fail.compareAndSet(null, e);
                }
            });
        }

        for (Thread worker : workers)
            worker.start();

        Thread.sleep(3_000);

        // Interrupts should not affect writes.
        for (Thread worker : workers)
            worker.interrupt();

        Thread.sleep(3_000);

        stop = true;

        for (Thread worker : workers)
            worker.join();

        Throwable t = fail.get();

        assertNull(t);

        IgniteCache<Integer, byte[]> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int verifiedKeys = 0;

        Map<Integer, byte[]> res = cache.getAll(keysToCheck);

        Assert.assertEquals(res.size(), keysToCheck.size());

        // Post check.
        for (Integer key: keysToCheck) {
            byte[] val = res.get(key);

            assertNotNull(val);
            assertEquals("Illegal length", VAL_LEN, val.length);

            verifiedKeys++;
        }

        log.info("Verified keys: " + verifiedKeys);
    }
}
