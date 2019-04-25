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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.io.File;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PrewarmingConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class PageMemoryPrewarmingTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    protected int maxMemorySize = 128 * 1024 * 1024;

    /** */
    protected int tmpFileMBytes = 2 * 1024;

    /** Size of int[] array values, x4 in bytes. */
    protected int valSize = 5 * 1024 * 1024;

    /** Value count. */
    protected int valCnt = 20;

    /** Wait warming up on start. */
    protected boolean waitPrewarmingOnStart;

    /** Warming up runtime dump delay. */
    protected long prewarmingRuntimeDumpDelay = 30_000;

    /** Ignite. */
    private volatile IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setPageSize(4 * 1024)
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(1024 * 1024 * 1024)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(maxMemorySize)
                .setInitialSize(maxMemorySize)
                .setPersistenceEnabled(true)
                .setPrewarmingConfiguration(new PrewarmingConfiguration()
                    .setWaitPrewarmingOnStart(waitPrewarmingOnStart)
                    .setRuntimeDumpDelay(prewarmingRuntimeDumpDelay))
            );

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(false);

        cleanPersistenceDir();

        U.delete(tmpDir());

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 600_000;
    }

    /**
     *
     */
    @Test
    public void testPrewarming() throws Exception {
        fillPersistence();

        ListeningTestLogger log = new ListeningTestLogger(false, log());

        LogListener throttleLsnr = throttleListener(false);

        log.registerListener(throttleLsnr);

        IgniteEx ignite = startGrid(getConfiguration(getTestIgniteInstanceName(0)).setGridLogger(log));

        pushOutDiskCache();

        IgniteCache<Integer, int[]> cache = ignite.getOrCreateCache(CACHE_NAME);

        for (int i = valCnt; i >= 0; i--) {
            long startTs = U.currentTimeMillis();

            int key = i % valCnt; // Key '0' supposed as cold.

            int[] val = cache.get(key);

            info("### " + key + " get in " + (U.currentTimeMillis() - startTs) + " ms, val=" +
                (val != null ? val.getClass().getSimpleName() + " [" + val.length + "]" : null));
        }

        assertTrue(throttleLsnr.check());
    }

    /**
     *
     */
    @Test
    public void testWarmingUpWithLoad() throws Exception {
        fillPersistence();

        AtomicBoolean stop = new AtomicBoolean(false);

        ListeningTestLogger log = new ListeningTestLogger(false, log());

        LogListener throttleLsnr = throttleListener(true);
        LogListener stopLsnr = LogListener.matches("Warming-up of DataRegion [name=default] finished in ")
            .times(1).build();

        log.registerListener(throttleLsnr);
        log.registerListener(stopLsnr);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0)).setGridLogger(log);

        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().getPrewarmingConfiguration()
            .setThrottleAccuracy(0.9)
            .setDumpReadThreads(1);

        GridTestUtils.runMultiThreadedAsync(getLoadRunnable(stop), 10, "put-thread");

        boolean res = false;

        try {
            for (int i = 0; i < 10 && !res; i++) {
                try {
                    stopLsnr.reset();
                    throttleLsnr.reset();

                    ignite = startGrid(cfg);

                    res = GridTestUtils.waitForCondition(stopLsnr::check, 180_000) && throttleLsnr.check();
                }
                catch (Throwable t) {
                    Thread.interrupted();
                }
            }

            assertTrue(res);
        }
        finally {
            stop.set(true);

            ignite.close();
        }
    }

    /**
     * Start node, put some vals into persistence and turn off the node.
     *
     * @throws Exception if failed.
     */
    private void fillPersistence() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, int[]> cache = ignite.getOrCreateCache(CACHE_NAME);

        int[] val = new int[valSize];

        for (int i = 0; i < valCnt; i++) {
            Arrays.fill(val, i);

            cache.put(i, val);
        }

        forceCheckpoint(ignite);

        ignite.close();
    }

    /**
     * @param expectThrottle Expect throttle.
     * @return LogListener expecting throttle message.
     */
    private LogListener throttleListener(boolean expectThrottle) {
        LogListener.Builder builder = LogListener.matches("Detected need to throttle warming up.");

        if (expectThrottle)
            return builder.atLeast(1).build();

        return builder.times(0).build();
    }

    /**
     * @param stop Stop flag.
     */
    private Runnable getLoadRunnable(AtomicBoolean stop) {
        return new Runnable() {
            @Override public void run() {
                Random r = new Random();

                int[] val = new int[valSize/2];

                Arrays.fill(val, r.nextInt(10));

                while (!stop.get()) {
                    boolean isIgniteAvailable = false;

                    try {
                        isIgniteAvailable = ignite != null && ignite.cluster().active();
                    }
                    catch (Throwable ignore) {}

                    if (!isIgniteAvailable)
                        continue;

                    IgniteCache<Integer, int[]> cache0 = ignite.getOrCreateCache(CACHE_NAME);

                    try {
                        while (!stop.get()) {
                            int k = valCnt - r.nextInt(valCnt / 2);

                            info("put " + k);

                            if (cache0.get(k) == null)
                                cache0.put(k, val);
                            else
                                cache0.remove(k);

                            IgniteKernal primaryNode = (IgniteKernal)primaryCache(k, CACHE_NAME).unwrap(Ignite.class);
                            GridCacheEntryEx entry = primaryNode.internalCache(CACHE_NAME).entryEx(k);

                            try {
                                entry.unswap();
                            }
                            catch (IgniteCheckedException | GridCacheEntryRemovedException ignore) {
                            }
                        }
                    }
                    catch (Throwable ignore) {}
                }
            }
        };
    }

    /**
     *
     */
    private void pushOutDiskCache() throws Exception {
        File tmp = new File(tmpDir(), "dummy.tmp");

        byte[] buf = new byte[1024 * 1024]; // 1MiB

        Arrays.fill(buf, (byte)0xFF);

        try (FileIO io = new RandomAccessFileIO(tmp,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)){

            for (int i = 0; i < tmpFileMBytes; i++) {
                io.write(buf, 0, buf.length);
                io.force();
            }

            U.warn(log, "Temp file written: " + tmp.getAbsolutePath() + ", size: " + io.size());
        }

        try (FileIO io = new RandomAccessFileIO(tmp, StandardOpenOption.READ)) {
            for (int i = 0; i < tmpFileMBytes; i++)
                io.read(buf, 0, buf.length);

            U.warn(log, "Temp file read: " + tmp.getAbsolutePath() + ", size: " + io.size());
        }
    }

    /**
     * @return Temporary directory.
     */
    private File tmpDir() throws IgniteCheckedException {
        return U.resolveWorkDirectory(U.defaultWorkDirectory(), "tmp", false);
    }
}
