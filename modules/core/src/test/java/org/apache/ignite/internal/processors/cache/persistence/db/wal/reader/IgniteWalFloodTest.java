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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.LongAdder8;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgniteWalFloodTest extends GridCommonAbstractTest {
    /** */
    private static final String HAS_CACHE = "HAS_CACHE";
    public static final String CACHE_NAME = "partitioned";

    /** */
    private boolean setWalArchAndWorkToSameValue = false;

    /** */
    private String cacheName;

    /** */
    private int walSegmentSize = 64 * 1024 * 1024;
    private WALMode customWalMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, IndexedObject> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setNodeFilter(new RemoteNodeFilter());
        ccfg.setIndexedTypes(Integer.class, IndexedObject.class);

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setPageSize(4 * 1024);

        DataRegionConfiguration memPlcCfg = new DataRegionConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setMaxSize(MemoryConfiguration.DFLT_MEMORY_POLICY_MAX_SIZE * 2);
        memPlcCfg.setPersistenceEnabled(true);

        dsCfg.setDefaultDataRegionConfiguration(memPlcCfg);

        if (walSegmentSize != 0)
            dsCfg.setWalSegmentSize(walSegmentSize);

        dsCfg.setCheckpointFrequency(10 * 1000);

        //dsCfg.setCheckpointPageBufferSize(1024L * 1024 * 1024);

        final String workDir = U.defaultWorkDirectory();
        final File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);
        final File wal = new File(db, "wal");
        if(setWalArchAndWorkToSameValue) {
            final String walAbsPath = wal.getAbsolutePath();

            dsCfg.setWalPath(walAbsPath);

            dsCfg.setWalArchivePath(walAbsPath);
        } else {
            dsCfg.setWalPath(wal.getAbsolutePath());

            dsCfg.setWalArchivePath(new File(wal, "archive").getAbsolutePath());
        }

        dsCfg.setWalMode(customWalMode!=null?customWalMode : WALMode.DEFAULT);
        dsCfg.setWalHistorySize(1);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setMarshaller(null);

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);

        if (!getTestIgniteInstanceName(0).equals(gridName))
            cfg.setUserAttributes(F.asMap(HAS_CACHE, true));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {

        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "temp", false));

        cacheName = CACHE_NAME;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        // deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));

    }

    /** Object with additional 40 000 bytes of payload */
    public static class HugeIndexedObject extends IndexedObject {
        byte[] data;

        /**
         * @param iVal Integer value.
         */
        private HugeIndexedObject(int iVal) {
            super(iVal);
            int sz = 40000;
            data = new byte[sz];
            for (int i = 0; i < sz; i++)
                data[i] = (byte)('A' + (i % 10));
        }
    }

    private static String generateThreadDump() {
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        final StringBuilder dump = new StringBuilder();
        for (ThreadInfo threadInfo : threadInfos) {
            dump.append(threadInfo.toString());
        }
        return dump.toString();
    }

    private static class ProgressWatchdog {

        private final LongAdder8 longAdder8 = new LongAdder8();

        private ScheduledExecutorService svc = Executors.newScheduledThreadPool(1);

        public void start() {
            final AtomicLong prevCnt = new AtomicLong();
            final AtomicLong prevMsElapsed = new AtomicLong();
            final long msStart = U.currentTimeMillis();
            prevMsElapsed.set(0);
            int checkPeriodSec = 1;
            svc.scheduleAtFixedRate(new Runnable() {
                @Override public void run() {
                    long elapsedMs = U.currentTimeMillis() - msStart;
                    final long totalCnt = longAdder8.longValue();
                    final long averagePutPerSec = totalCnt * 1000 / elapsedMs;
                    final long currPutPerSec = ((totalCnt - prevCnt.getAndSet(totalCnt)) * 1000) / (elapsedMs - prevMsElapsed.getAndSet(elapsedMs));
                    final String fileNameWithDump  = currPutPerSec == 0 ? reactNoProgress(msStart) : "";
                    X.println(" >> Written " + totalCnt + ", time " + elapsedMs + " ms," +
                        " Average put " + averagePutPerSec + "/sec, current put " + currPutPerSec + "/sec " + fileNameWithDump);

                }
            }, checkPeriodSec, checkPeriodSec, TimeUnit.SECONDS);
        }

        private String reactNoProgress(long msStart) {
            try {
                String s = generateThreadDump();
                long sec = (U.currentTimeMillis() - msStart) / 1000;
                String fileName = "dumpAt" + sec + "second.txt";
                if (s.contains(IgniteCacheDatabaseSharedManager.class.getName() + ".checkpointLock"))
                    fileName = "checkpoint_" + fileName;

                File tempDir = new File(U.defaultWorkDirectory(), "temp");
                tempDir.mkdirs();
                try (FileWriter writer = new FileWriter(new File(tempDir , fileName))) {
                    writer.write(s);
                }
                return fileName;
            }
            catch (IOException | IgniteCheckedException e) {
                e.printStackTrace();
            }
            return "";
        }

        public void reportProgress(int cnt) {
            longAdder8.add(cnt);
        }

        public void stop() {
            svc.shutdown();
            try {
                svc.awaitTermination(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }

    public void ignoreTestContinuousPutMultithreaded() throws Exception {
        try {
            final IgniteEx ignite = startGrid(1);

            final IgniteCache<Object, IndexedObject> cache = ignite.cache(CACHE_NAME);
            int totalRecs = 200_000;
            final int threads = 10;

            final int recsPerThread = totalRecs / threads;
            final Collection<Callable<?>> tasks = new ArrayList<>();

            final ProgressWatchdog watchdog = new ProgressWatchdog();

            for (int j = 0; j < threads; j++) {
                final IgniteCache<Object, IndexedObject> finalCache = cache;
                final int finalJ = j;
                tasks.add(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        for (int i = finalJ * recsPerThread; i < ((finalJ + 1) * recsPerThread); i++) {
                            IndexedObject v = new HugeIndexedObject(i);
                            finalCache.put(i, v);
                            watchdog.reportProgress(1);
                        }
                        return null;
                    }
                });
            }

            watchdog.start();
            GridTestUtils.runMultiThreaded(tasks, "put-thread");
            watchdog.stop();
            stopGrid(1);

            final Ignite restartedIgnite = startGrid(1);

            final IgniteCache<Object, IndexedObject> restartedCache = restartedIgnite.cache(CACHE_NAME);

            // Check.
            for (int i = 0; i < recsPerThread * threads; i++) {
                TestCase.assertEquals(i, restartedCache.get(i).iVal);
                if (i % 1000 == 0)
                    X.println(" >> Verified: " + i);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    private static boolean keepInDb(int id) {
        return id % 1777==0;
    }

    public void testPutRemoveMultithread() throws Exception {
        setWalArchAndWorkToSameValue = true;

        //if (!setWalArchAndWorkToSameValue)
        //    assertNull(getConfiguration("").getDataStorageConfiguration().getWalArchivePath());

        customWalMode = WALMode.LOG_ONLY;

        try {
            final IgniteEx ignite = startGrid(1);
            ignite.active(true);

            final IgniteCache<Object, IndexedObject> cache = ignite.cache(CACHE_NAME);
            int totalRecs = 200_000;
            final int threads = 10;

            final int recsPerThread = totalRecs / threads;
            final Collection<Callable<?>> tasks = new ArrayList<>();

            final ProgressWatchdog watchdog = new ProgressWatchdog();

            for (int j = 0; j < threads; j++) {
                final IgniteCache<Object, IndexedObject> finalCache = cache;
                final int finalJ = j;
                tasks.add(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        final List<Integer> toRmvLaterList = new ArrayList<>();
                        for (int id = finalJ * recsPerThread; id < ((finalJ + 1) * recsPerThread); id++) {
                            IndexedObject v = new HugeIndexedObject(id);
                            finalCache.put(id, v);
                            toRmvLaterList.add(id);
                            watchdog.reportProgress(1);
                            if(toRmvLaterList.size()>100) {
                                for (Integer toRemoveId : toRmvLaterList) {
                                    if(keepInDb(toRemoveId))
                                        continue;
                                    boolean rmv = finalCache.remove(toRemoveId);
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
            GridTestUtils.runMultiThreaded(tasks, "put-thread");
            watchdog.stop();
            stopGrid(1);

            final Ignite restartedIgnite = startGrid(1);
            restartedIgnite.active(true);

            final IgniteCache<Object, IndexedObject> restartedCache = restartedIgnite.cache(CACHE_NAME);

            // Check.
            for (int i = 0; i < recsPerThread * threads; i++) {
                if(keepInDb(i)) {
                    final IndexedObject obj = restartedCache.get(i);

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

    /**
     *
     */
    private static class RemoteNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return clusterNode.attribute(HAS_CACHE) != null;
        }
    }

    /**
     *
     */
    private static class IndexedObject {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /**
         * @param iVal Integer value.
         */
        private IndexedObject(int iVal) {
            this.iVal = iVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof IndexedObject))
                return false;

            IndexedObject that = (IndexedObject)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexedObject.class, this);
        }
    }
}