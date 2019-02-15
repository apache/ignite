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

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test what interruptions of writing threads do not affect PDS.
 */
@RunWith(JUnit4.class)
public class IgnitePdsThreadInterruptionTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = 1 << 12; // 4096

    /** */
    public static final int THREADS_CNT = 100;

    /**
     * Cache name.
     */
    private final String CACHE_NAME = "cache";

    /** */
    private volatile boolean stop = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDataStorageConfiguration(storageConfiguration());

        CacheConfiguration ccfg = new CacheConfiguration<>(CACHE_NAME);

        RendezvousAffinityFunction affinityFunction = new RendezvousAffinityFunction();
        affinityFunction.setPartitions(1);

        ccfg.setAffinity(affinityFunction);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return DataStorage configuration.
     */
    private DataStorageConfiguration storageConfiguration() {
        DataRegionConfiguration regionCfg = new DataRegionConfiguration()
                .setInitialSize(10L * 1024L * 1024L)
                .setMaxSize(10L * 1024L * 1024L)
                .setPageEvictionMode(DataPageEvictionMode.RANDOM_LRU);

        DataStorageConfiguration cfg = new DataStorageConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
                .setWalFsyncDelayNanos(0)
                .setPageSize(PAGE_SIZE)
                .setFileIOFactory(new AsyncFileIOFactory());

        cfg.setDefaultDataRegionConfiguration(regionCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests interruptions on LFS read.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInterruptsOnLFSRead() throws Exception {
        final Ignite ignite = startGrid();

        ignite.active(true);

        final int valLen = 8192;

        final byte[] payload = new byte[valLen];

        final int maxKey = 10_000;

        Thread[] workers = new Thread[THREADS_CNT];


        final IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

        for (int i=0; i < maxKey; i++)
            cache.put(i, payload);

        final AtomicReference<Throwable> fail = new AtomicReference<>();


        Runnable clo = new Runnable() {
            @Override public void run() {
                cache.get(ThreadLocalRandom.current().nextInt(maxKey / 5));
            }
        };

        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Thread(clo);
            workers[i].setName("reader-" + i);
            workers[i].setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override public void uncaughtException(Thread t, Throwable e) {
                    fail.compareAndSet(null, e);
                }
            });
        }

        for (Thread worker : workers)
            worker.start();

        //Thread.sleep(3_000);

        // Interrupts should not affect reads.
        for (int i = 0;i < workers.length / 2; i++)
            workers[i].interrupt();

        Thread.sleep(3_000);

        stop = true;

        for (Thread worker : workers)
            worker.join();

        Throwable t = fail.get();

        assertNull(t);

        int verifiedKeys = 0;

        // Post check.
        for (int i = 0; i < maxKey; i++) {
            byte[] val = (byte[]) cache.get(i);

            if (val != null) {
                assertEquals("Illegal length", valLen, val.length);

                verifiedKeys++;
            }
        }

        log.info("Verified keys: " + verifiedKeys);
    }

    /**
     * Tests interruptions on WAL write.
     *
     * @throws Exception
     */
    @Test
    public void testInterruptsOnWALWrite() throws Exception {
        final Ignite ignite = startGrid();

        ignite.active(true);

        final int valLen = 8192;

        final byte[] payload = new byte[valLen];

        final int maxKey = 100_000;

        Thread[] workers = new Thread[THREADS_CNT];

        final AtomicReference<Throwable> fail = new AtomicReference<>();

        Runnable clo = new Runnable() {
            @Override public void run() {
                IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

                while (!stop)
                    cache.put(ThreadLocalRandom.current().nextInt(maxKey), payload);
            }
        };

        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Thread(clo);
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

        IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

        int verifiedKeys = 0;

        // Post check.
        for (int i = 0; i < maxKey; i++) {
            byte[] val = (byte[]) cache.get(i);

            if (val != null) {
                assertEquals("Illegal length", valLen, val.length);

                verifiedKeys++;
            }
        }

        log.info("Verified keys: " + verifiedKeys);
    }
}
