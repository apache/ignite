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

package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.cache.Cache;
import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Sandbox test to measure progress of grid write operations. If no progress occur during period of time, then thread
 * dumps are generated.
 */
public class IgniteMassLoadSandboxTest extends GridCommonAbstractTest {
    /** Cache name. Random to cover external stores possible problems. */
    public static final String CACHE_NAME = "partitioned" + new Random().nextInt(10000000);

    /** Object size - minimal size of object to be placed in cache. */
    private static final int OBJECT_SIZE = 40000;

    /** Records count to continuous put into cache. */
    private static final int CONTINUOUS_PUT_RECS_CNT = 300_000;

    /** Put thread: client threads naming for put operation. */
    private static final String PUT_THREAD = "put-thread";

    /** Get thread: client threadsd naming for verify operation. */
    private static final String GET_THREAD = "get-thread";

    /** Option to enabled storage verification after test. */
    private static final boolean VERIFY_STORAGE = false;

    /**
     * Set WAL archive and work folders to same value.  Activates 'No Archiver' mode.
     * See {@link FileWriteAheadLogManager#isArchiverEnabled()}.
     */
    private boolean setWalArchAndWorkToSameVal;

    /** Option for test run: WAL segments size in bytes. */
    private int walSegmentSize = 64 * 1024 * 1024;

    /** Option for test run: Custom WAL mode. */
    private WALMode customWalMode;

    /** Option for test run: Checkpoint frequency. */
    private int checkpointFrequency = 40 * 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, HugeIndexedObject> ccfg = new CacheConfiguration<>();

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1024));
        ccfg.setIndexedTypes(Integer.class, HugeIndexedObject.class);
        ccfg.setName(CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        DataRegionConfiguration regCfg = new DataRegionConfiguration()
            .setName("dfltMemPlc")
            .setMetricsEnabled(true)
            .setMaxSize(2 * 1024L * 1024 * 1024)
            .setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setDefaultDataRegionConfiguration(regCfg)
            .setPageSize(4 * 1024)
            .setWriteThrottlingEnabled(true)
            .setCheckpointFrequency(checkpointFrequency);

        final String workDir = U.defaultWorkDirectory();
        final File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);
        final File wal = new File(db, "wal");
        if (setWalArchAndWorkToSameVal) {
            final String walAbsPath = wal.getAbsolutePath();

            dsCfg.setWalPath(walAbsPath);

            dsCfg.setWalArchivePath(walAbsPath);
        }
        else {
            dsCfg.setWalPath(wal.getAbsolutePath());

            dsCfg.setWalArchivePath(new File(wal, "archive").getAbsolutePath());
        }

        dsCfg.setWalMode(customWalMode != null ? customWalMode : WALMode.LOG_ONLY)
            .setWalHistorySize(1)
            .setWalSegments(10);

        if (walSegmentSize != 0)
            dsCfg.setWalSegmentSize(walSegmentSize);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "temp", false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Runs multithreaded put scenario (no data streamer). Load is generated to page store and to WAL.
     *
     * @throws Exception if failed.
     */
    public void testContinuousPutMultithreaded() throws Exception {
        try {
            // System.setProperty(IgniteSystemProperties.IGNITE_DIRTY_PAGES_PARALLEL, "true");
            // System.setProperty(IgniteSystemProperties.IGNITE_DIRTY_PAGES_SORTED_STORAGE, "true");
            System.setProperty(IgniteSystemProperties.IGNITE_USE_ASYNC_FILE_IO_FACTORY, "false");
            System.setProperty(IgniteSystemProperties.IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED, "speed");

            setWalArchAndWorkToSameVal = true;

            customWalMode = WALMode.BACKGROUND;

            final IgniteEx ignite = startGrid(1);

            ignite.active(true);

            final IgniteCache<Object, HugeIndexedObject> cache = ignite.cache(CACHE_NAME);
            final int threads = Runtime.getRuntime().availableProcessors();

            final int recsPerThread = CONTINUOUS_PUT_RECS_CNT / threads;

            final Collection<Callable<?>> tasks = new ArrayList<>();

            final ProgressWatchdog watchdog = new ProgressWatchdog(ignite, "put", PUT_THREAD);

            for (int j = 0; j < threads; j++) {
                final int finalJ = j;

                tasks.add(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        for (int i = finalJ * recsPerThread; i < ((finalJ + 1) * recsPerThread); i++) {
                            HugeIndexedObject v = new HugeIndexedObject(i);
                            cache.put(i, v);
                            watchdog.reportProgress(1);
                        }
                        return null;
                    }
                });
            }

            watchdog.start();
            GridTestUtils.runMultiThreaded(tasks, PUT_THREAD);

            watchdog.stopping();
            stopGrid(1);

            watchdog.stop();

            if (VERIFY_STORAGE)
                runVerification(threads, recsPerThread);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Runs multithreaded put scenario (no data streamer). Load is generated to page store and to WAL.
     *
     * @throws Exception if failed.
     */
    public void testDataStreamerContinuousPutMultithreaded() throws Exception {
        try {
            // System.setProperty(IgniteSystemProperties.IGNITE_DIRTY_PAGES_PARALLEL, "true");
            // System.setProperty(IgniteSystemProperties.IGNITE_DIRTY_PAGES_SORTED_STORAGE, "true");
            System.setProperty(IgniteSystemProperties.IGNITE_USE_ASYNC_FILE_IO_FACTORY, "false");
            System.setProperty(IgniteSystemProperties.IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED, "speed");
            System.setProperty(IgniteSystemProperties.IGNITE_DELAYED_REPLACED_PAGE_WRITE, "true");

            setWalArchAndWorkToSameVal = true;

            customWalMode = WALMode.BACKGROUND;

            final IgniteEx ignite = startGrid(1);

            ignite.active(true);

            final int threads = 1;
            Runtime.getRuntime().availableProcessors();

            final int recsPerThread = CONTINUOUS_PUT_RECS_CNT / threads;

            final ProgressWatchdog watchdog = new ProgressWatchdog(ignite, "put", PUT_THREAD);

            IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(CACHE_NAME);

            streamer.perNodeBufferSize(12);

            final Collection<Callable<?>> tasks = new ArrayList<>();
            for (int j = 0; j < threads; j++) {
                final int finalJ = j;

                tasks.add((Callable<Void>)() -> {
                    for (int i = finalJ * recsPerThread; i < ((finalJ + 1) * recsPerThread); i++)
                        streamer.addData(i, new HugeIndexedObject(i));

                    return null;
                });
            }

            final IgniteCache<Object, HugeIndexedObject> cache = ignite.cache(CACHE_NAME);
            ScheduledExecutorService svcReport = Executors.newScheduledThreadPool(1);

            AtomicInteger size = new AtomicInteger();
            svcReport.scheduleAtFixedRate(
                () -> {
                    int newSize = cache.size();
                    int oldSize = size.getAndSet(newSize);

                    watchdog.reportProgress(newSize - oldSize);
                },
                250, 250, TimeUnit.MILLISECONDS);

            watchdog.start();
            GridTestUtils.runMultiThreaded(tasks, PUT_THREAD);
            streamer.close();

            watchdog.stopping();
            stopGrid(1);

            watchdog.stop();

            ProgressWatchdog.stopPool(svcReport);

            if (VERIFY_STORAGE)
                runVerification(threads, recsPerThread);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test that WAL segments that are fully covered by checkpoint are logged
     *
     * @throws Exception if failed.
     */
    public void testCoveredWalLogged() throws Exception {
        GridStringLogger log0 = null;

        try {
            log0 = new GridStringLogger();

            final IgniteConfiguration cfg = getConfiguration("testCoveredWalLogged");

            cfg.setGridLogger(log0);

            cfg.getDataStorageConfiguration().setWalAutoArchiveAfterInactivity(10);

            final Ignite ignite = G.start(cfg);

            ignite.cluster().active(true);

            final IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

            cache.put(1, new byte[cfg.getDataStorageConfiguration().getWalSegmentSize() - 1024]);

            forceCheckpoint();

            cache.put(1, new byte[cfg.getDataStorageConfiguration().getWalSegmentSize() - 1024]);

            forceCheckpoint();

            cache.put(1, new byte[cfg.getDataStorageConfiguration().getWalSegmentSize() - 1024]);

            forceCheckpoint();

            Thread.sleep(200); // needed by GridStringLogger

            final String log = log0.toString();

            final String lines[] = log.split("\\r?\\n");

            final Pattern chPtrn = Pattern.compile("Checkpoint finished");

            final Pattern idxPtrn = Pattern.compile("idx=([0-9]+),");

            final Pattern covererdPtrn = Pattern.compile("walSegmentsCovered=\\[(.+)\\], ");

            boolean hasCheckpoint = false;

            long nextCovered = 0;

            for (String line : lines) {
                if (!chPtrn.matcher(line).find())
                    continue;

                hasCheckpoint = true;

                final Matcher idxMatcher = idxPtrn.matcher(line);

                assertTrue(idxMatcher.find());

                final long idx = Long.valueOf(idxMatcher.group(1));

                final Matcher coveredMatcher = covererdPtrn.matcher(line);

                if (!coveredMatcher.find()) { // no wal segments are covered by checkpoint
                    assertEquals(nextCovered, idx);
                    continue;
                }

                final String coveredMatcherGrp = coveredMatcher.group(1);

                final long[] covered = coveredMatcherGrp.length() > 0 ?
                    Arrays.stream(coveredMatcherGrp.split(" - ")).mapToLong(e -> Integer.valueOf(e.trim())).toArray() :
                    new long[0];

                assertEquals(nextCovered, covered[0]);

                final long lastCovered = covered[covered.length - 1];

                assertEquals(idx - 1, lastCovered);  // current wal is excluded

                nextCovered = lastCovered + 1;
            }

            assertTrue(hasCheckpoint);

        }
        finally {
            System.out.println(log0 != null ? log0.toString() : "Error initializing GridStringLogger");

            stopAllGrids();
        }
    }

    /**
     * Verifies data from storage.
     *
     * @param threads threads count.
     * @param recsPerThread record per thread loaded.
     * @throws Exception if failed
     */
    private void runVerification(int threads, final int recsPerThread) throws Exception {
        final Ignite restartedIgnite = startGrid(1);

        restartedIgnite.active(true);

        final IgniteCache<Integer, HugeIndexedObject> restartedCache = restartedIgnite.cache(CACHE_NAME);

        final ProgressWatchdog watchdog2 = new ProgressWatchdog(restartedIgnite, "get", GET_THREAD);

        final Collection<Callable<?>> tasksR = new ArrayList<>();
        tasksR.clear();
        for (int j = 0; j < threads; j++) {
            final int finalJ = j;
            tasksR.add(new Callable<Void>() {
                @Override public Void call() {
                    for (int i = finalJ * recsPerThread; i < ((finalJ + 1) * recsPerThread); i++) {
                        HugeIndexedObject obj = restartedCache.get(i);
                        int actVal = obj.iVal;
                        TestCase.assertEquals(i, actVal);
                        watchdog2.reportProgress(1);
                    }
                    return null;
                }
            });
        }

        watchdog2.start();
        GridTestUtils.runMultiThreaded(tasksR, GET_THREAD);
        watchdog2.stop();
    }

    /**
     * @param threads Threads count.
     * @param recsPerThread initial records per thread.
     * @param restartedCache cache to obtain data from.
     */
    private void verifyByChunk(int threads, int recsPerThread, Cache<Integer, HugeIndexedObject> restartedCache) {
        int verifyChunk = 100;

        int totalRecsToVerify = recsPerThread * threads;
        int chunks = totalRecsToVerify / verifyChunk;

        for (int c = 0; c < chunks; c++) {
            Set<Integer> keys = new TreeSet<>();

            for (int i = 0; i < verifyChunk; i++)
                keys.add(i + c * verifyChunk);

            Map<Integer, HugeIndexedObject> values = restartedCache.getAll(keys);

            for (Map.Entry<Integer, HugeIndexedObject> next : values.entrySet()) {
                Integer key = next.getKey();

                int actVal = values.get(next.getKey()).iVal;
                int i = key;
                TestCase.assertEquals(i, actVal);

                if (i % 1000 == 0)
                    X.println(" >> Verified: " + i);
            }

        }
    }

    /**
     * @param id entry id.
     * @return {@code True} if need to keep entry in DB and checkpoint it. Most of entries not required.
     */
    private static boolean keepInDb(int id) {
        return id % 1777 == 0;
    }

    /**
     * Runs multithreaded put-remove scenario (no data streamer). Load is generated to WAL log mostly.
     * Most of entries generated will be removed before first checkpoint.
     *
     * @throws Exception if failed.
     */
    public void testPutRemoveMultithreaded() throws Exception {
        setWalArchAndWorkToSameVal = false;
        customWalMode = WALMode.LOG_ONLY;

        try {
            final IgniteEx ignite = startGrid(1);

            ignite.active(true);
            checkpointFrequency = 20 * 1000;
            final IgniteCache<Object, HugeIndexedObject> cache = ignite.cache(CACHE_NAME);
            int totalRecs = 400_000;
            final int threads = 10;
            final int recsPerThread = totalRecs / threads;
            final Collection<Callable<?>> tasks = new ArrayList<>();
            final ProgressWatchdog watchdog = new ProgressWatchdog(ignite, "put", PUT_THREAD);

            for (int j = 0; j < threads; j++) {
                final int finalJ = j;

                tasks.add(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        final Collection<Integer> toRmvLaterList = new ArrayList<>();

                        for (int id = finalJ * recsPerThread; id < ((finalJ + 1) * recsPerThread); id++) {
                            HugeIndexedObject v = new HugeIndexedObject(id);

                            cache.put(id, v);
                            toRmvLaterList.add(id);
                            watchdog.reportProgress(1);

                            if (toRmvLaterList.size() > 100) {
                                for (Integer toRemoveId : toRmvLaterList) {
                                    if (keepInDb(toRemoveId))
                                        continue;

                                    boolean rmv = cache.remove(toRemoveId);
                                    assert rmv : "Expected to remove object from cache " + toRemoveId;
                                }
                                toRmvLaterList.clear();
                            }
                        }
                        return null;
                    }
                });
            }

            watchdog.start();
            GridTestUtils.runMultiThreaded(tasks, PUT_THREAD);
            watchdog.stop();
            stopGrid(1);

            final Ignite restartedIgnite = startGrid(1);

            restartedIgnite.active(true);

            final IgniteCache<Object, HugeIndexedObject> restartedCache = restartedIgnite.cache(CACHE_NAME);

            for (int i = 0; i < recsPerThread * threads; i++) {
                if (keepInDb(i)) {
                    final HugeIndexedObject obj = restartedCache.get(i);

                    TestCase.assertNotNull(obj);
                    TestCase.assertEquals(i, obj.iVal);
                }

                if (i % 1000 == 0)
                    X.print(" V: " + i);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TimeUnit.MINUTES.toMillis(20);
    }

    /** Object with additional 40 000 bytes of payload */
    public static class HugeIndexedObject {
        /** Data. */
        private byte[] data;
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /**
         * @param iVal Integer value.
         */
        private HugeIndexedObject(int iVal) {
            this.iVal = iVal;

            int sz = OBJECT_SIZE;

            data = new byte[sz];
            for (int i = 0; i < sz; i++)
                data[i] = (byte)('A' + (i % 10));
        }

        /**
         * @return Data.
         */
        public byte[] data() {
            return data;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof HugeIndexedObject))
                return false;

            HugeIndexedObject that = (HugeIndexedObject)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(HugeIndexedObject.class, this);
        }
    }
}
