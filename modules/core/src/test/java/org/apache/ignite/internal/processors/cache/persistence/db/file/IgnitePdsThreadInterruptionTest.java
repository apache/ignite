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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ThreadLocalRandom8;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Test what interruptions of writing threads do not affect PDS.
 */
public class IgnitePdsThreadInterruptionTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = 1 << 12; // 4096

    /** */
    public static final int THREADS_CNT = 10;

    /**
     * Cache name.
     */
    private final String cacheName = "cache";

    /** */
    private volatile boolean stop = false;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPersistentStoreConfiguration(storeConfiguration());

        cfg.setMemoryConfiguration(memoryConfiguration());

        cfg.setCacheConfiguration(new CacheConfiguration<>(cacheName));

        return cfg;
    }

    /**
     * @return Store config.
     */
    private PersistentStoreConfiguration storeConfiguration() {
        PersistentStoreConfiguration cfg = new PersistentStoreConfiguration();

        cfg.setWalMode(WALMode.LOG_ONLY);

        cfg.setWalFsyncDelayNanos(0);

        cfg.setFileIOFactory(new AsyncFileIOFactory());

        return cfg;
    }

    /**
     * @return Memory config.
     */
    private MemoryConfiguration memoryConfiguration() {
        final MemoryConfiguration memCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();
        // memPlcCfg.setPageEvictionMode(RANDOM_LRU); TODO Fix NPE on start.
        memPlcCfg.setName("dfltMemPlc");

        memCfg.setPageSize(PAGE_SIZE);
        memCfg.setConcurrencyLevel(1);
        memCfg.setMemoryPolicies(memPlcCfg);
        memCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        return memCfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        deleteWorkFiles();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        deleteWorkFiles();
    }

    /**
     * Tests interruptions on WAL write.
     *
     * @throws Exception
     */
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
                IgniteCache<Object, Object> cache = ignite.cache(cacheName);

                while (!stop)
                    cache.put(ThreadLocalRandom8.current().nextInt(maxKey), payload);
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

        assert t == null : t;

        IgniteCache<Object, Object> cache = ignite.cache(cacheName);

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
     * @throws IgniteCheckedException If fail.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }
}