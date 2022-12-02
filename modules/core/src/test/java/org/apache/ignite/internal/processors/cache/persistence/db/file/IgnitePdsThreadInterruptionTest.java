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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.util.IgniteUtils.MB;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

/**
 * Test what interruptions of writing threads do not affect PDS.
 */
public class IgnitePdsThreadInterruptionTest extends GridCommonAbstractTest {
    /** */
    public static final int THREADS_CNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setWalFsyncDelayNanos(0)
            .setWalSegmentSize((int)MB)
            .setWalSegments(2)
            .setFileIOFactory(new AsyncFileIOFactory())
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setInitialSize(10L * MB)
                    .setMaxSize(10L * MB)
            ));

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 1))
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /**
     * Tests interruptions on read.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInterruptsOnRead() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.cluster().state(ACTIVE);

        int keyCount = 10_000;

        byte[] value = new byte[8_192];

        // Load data.
        try (IgniteDataStreamer<Integer, byte[]> st = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < keyCount; i++)
                st.addData(i, value);
        }

        IgniteCache<Integer, byte[]> cache = ignite.cache(DEFAULT_CACHE_NAME);

        Collection<Throwable> readThreadsError = new ConcurrentLinkedQueue<>();

        CountDownLatch startThreadsLatch = new CountDownLatch(THREADS_CNT);

        Thread[] workers = new Thread[THREADS_CNT];

        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Thread(
                () -> {
                    try {
                        startThreadsLatch.countDown();

                        cache.get(ThreadLocalRandom.current().nextInt(keyCount / 5));
                    }
                    catch (Throwable throwable) {
                        if (!X.hasCause(
                            throwable,
                            "Failed to wait for asynchronous operation permit",
                            IgniteInterruptedException.class
                        ))
                            readThreadsError.add(throwable);
                    }
                },
                "cache-reader-from-test" + i
            );
        }

        for (Thread worker : workers)
            worker.start();

        assertTrue(startThreadsLatch.await(3, TimeUnit.SECONDS));

        // Interrupts should not affect reads.
        for (Thread worker : workers)
            worker.interrupt();

        for (Thread worker : workers)
            worker.join(TimeUnit.SECONDS.toMillis(1));

        assertThat(readThreadsError, empty());

        for (int i = 0; i < keyCount; i++)
            assertArrayEquals(String.valueOf(i), cache.get(i), value);
    }

    /**
     * Tests interruptions on WAL write.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInterruptsOnWALWrite() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.cluster().state(ACTIVE);

        IgniteCache<Integer, byte[]> cache = ignite.cache(DEFAULT_CACHE_NAME);

        Set<Integer> keysToCheck = new GridConcurrentHashSet<>();

        Collection<Throwable> writeThreadsError = new ConcurrentLinkedQueue<>();

        AtomicBoolean stop = new AtomicBoolean();

        byte[] value = new byte[8_192];

        Thread[] workers = new Thread[THREADS_CNT];

        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        int key = ThreadLocalRandom.current().nextInt(100_000);

                        cache.put(key, value);

                        keysToCheck.add(key);
                    }
                }
                catch (Throwable throwable) {
                    writeThreadsError.add(throwable);
                }
            }, "cache-writer-from-test" + i);

            workers[i].setName("writer-" + i);
        }

        for (Thread worker : workers)
            worker.start();

        Thread.sleep(3_000);

        // Interrupts should not affect writes.
        for (Thread worker : workers)
            worker.interrupt();

        Thread.sleep(3_000);

        stop.set(true);

        for (Thread worker : workers)
            worker.join(TimeUnit.SECONDS.toMillis(1));

        assertThat(writeThreadsError, empty());

        // Post check.
        for (Integer key: keysToCheck)
            assertArrayEquals(String.valueOf(key), value, cache.get(key));
    }
}
