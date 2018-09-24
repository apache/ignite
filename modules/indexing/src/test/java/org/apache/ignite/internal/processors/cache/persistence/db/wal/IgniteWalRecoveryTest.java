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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.PAX;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Assert;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgniteWalRecoveryTest extends GridCommonAbstractTest {
    /** */
    private static final String HAS_CACHE = "HAS_CACHE";

    /** */
    private static final int LARGE_ARR_SIZE = 1025;

    /** */
    private boolean fork;

    /** */
    private static final String CACHE_NAME = "partitioned";

    /** */
    private static final String RENAMED_CACHE_NAME = "partitioned0";

    /** */
    private static final String LOC_CACHE_NAME = "local";

    /** */
    private boolean renamed;

    /** */
    private int walSegmentSize;

    /** Log only. */
    private boolean logOnly;

    /** */
    private long customFailureDetectionTimeout = -1;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return fork;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, IndexedObject> ccfg = renamed ?
            new CacheConfiguration<>(RENAMED_CACHE_NAME) : new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setNodeFilter(new RemoteNodeFilter());
        ccfg.setIndexedTypes(Integer.class, IndexedObject.class);

        CacheConfiguration<Integer, IndexedObject> locCcfg = new CacheConfiguration<>(LOC_CACHE_NAME);
        locCcfg.setCacheMode(CacheMode.LOCAL);
        locCcfg.setIndexedTypes(Integer.class, IndexedObject.class);

        cfg.setCacheConfiguration(ccfg, locCcfg);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg.setPageSize(4 * 1024);

        DataRegionConfiguration memPlcCfg = new DataRegionConfiguration();

        memPlcCfg.setName("dfltDataRegion");
        memPlcCfg.setInitialSize(1024L * 1024 * 1024);
        memPlcCfg.setMaxSize(1024L * 1024 * 1024);
        memPlcCfg.setPersistenceEnabled(true);

        dbCfg.setDefaultDataRegionConfiguration(memPlcCfg);

        dbCfg.setWalRecordIteratorBufferSize(1024 * 1024);

        dbCfg.setWalHistorySize(2);

        if (logOnly)
            dbCfg.setWalMode(WALMode.LOG_ONLY);

        if (walSegmentSize != 0)
            dbCfg.setWalSegmentSize(walSegmentSize);

        cfg.setDataStorageConfiguration(dbCfg);

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);

        if (!getTestIgniteInstanceName(0).equals(gridName))
            cfg.setUserAttributes(F.asMap(HAS_CACHE, true));

        if (customFailureDetectionTimeout > 0)
            cfg.setFailureDetectionTimeout(customFailureDetectionTimeout);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        renamed = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        logOnly = false;

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    public void testWalBig() throws Exception {
        IgniteEx ignite = startGrid(1);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

        Random rnd = new Random();

        Map<Integer, IndexedObject> map = new HashMap<>();

        for (int i = 0; i < 10_000; i++) {
            if (i % 1000 == 0)
                X.println(" >> " + i);

            int k = rnd.nextInt(300_000);
            IndexedObject v = new IndexedObject(rnd.nextInt(10_000));

            cache.put(k, v);
            map.put(k, v);
        }

        // Check.
        for (Integer k : map.keySet())
            assertEquals(map.get(k), cache.get(k));

        stopGrid(1);

        ignite = startGrid(1);

        ignite.cluster().active(true);

        cache = ignite.cache(CACHE_NAME);

        // Check.
        for (Integer k : map.keySet())
            assertEquals(map.get(k), cache.get(k));
    }

    /**
     * @throws Exception if failed.
     */
    public void testWalBigObjectNodeCancel() throws Exception {
        final int MAX_SIZE_POWER = 21;

        IgniteEx ignite = startGrid(1);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

        for (int i = 0; i < MAX_SIZE_POWER; ++i) {
            int size = 1 << i;

            cache.put("key_" + i, createTestData(size));
        }

        stopGrid(1, true);

        ignite = startGrid(1);

        ignite.cluster().active(true);

        cache = ignite.cache(CACHE_NAME);

        // Check.
        for (int i = 0; i < MAX_SIZE_POWER; ++i) {
            int size = 1 << i;

            int[] data = createTestData(size);

            int[] val = (int[])cache.get("key_" + i);

            assertTrue("Invalid data. [key=key_" + i + ']', Arrays.equals(data, val));
        }
    }

    /**
     * @throws Exception If fail.
     */
    public void testSwitchClassLoader() throws Exception {
        try {
            final IgniteEx igniteEx = startGrid(1);

            // CustomDiscoveryMessage will trigger service tasks
            startGrid(2);

            igniteEx.cluster().active(true);

            IgniteCache<Integer, EnumVal> cache = igniteEx.cache(CACHE_NAME);

            final ClassLoader oldCl = Thread.currentThread().getContextClassLoader();
            final ClassLoader newCl = getExternalClassLoader();

            Thread.currentThread().setContextClassLoader(newCl);

            for (int i = 0; i < 10; i++)
                cache.put(i, i % 2 == 0 ? EnumVal.VAL1 : EnumVal.VAL2);

            for (int i = 0; i < 10; i++)
                assert cache.containsKey(i);

            // Invokes ClearTask with new class loader
            cache.clear();

            Thread.currentThread().setContextClassLoader(oldCl);

            for (int i = 0; i < 10; i++)
                cache.put(i, i % 2 == 0 ? EnumVal.VAL1 : EnumVal.VAL2);

            for (int i = 0; i < 10; i++)
                assert cache.containsKey(i);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testWalSimple() throws Exception {
        try {
            IgniteEx ignite = startGrid(1);

            ignite.cluster().active(true);

            IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

            info(" --> step1");

            for (int i = 0; i < 10_000; i += 2)
                cache.put(i, new IndexedObject(i));

            info(" --> step2");

            for (int i = 0; i < 10_000; i += 3)
                cache.put(i, new IndexedObject(i * 2));

            info(" --> step3");

            for (int i = 0; i < 10_000; i += 7)
                cache.put(i, new IndexedObject(i * 3));

            info(" --> check1");

            // Check.
            for (int i = 0; i < 10_000; i++) {
                IndexedObject o;

                if (i % 7 == 0)
                    o = new IndexedObject(i * 3);
                else if (i % 3 == 0)
                    o = new IndexedObject(i * 2);
                else if (i % 2 == 0)
                    o = new IndexedObject(i);
                else
                    o = null;

                assertEquals(o, cache.get(i));
            }

            stopGrid(1);

            ignite = startGrid(1);

            ignite.cluster().active(true);

            cache = ignite.cache(CACHE_NAME);

            info(" --> check2");

            // Check.
            for (int i = 0; i < 10_000; i++) {
                IndexedObject o;

                if (i % 7 == 0)
                    o = new IndexedObject(i * 3);
                else if (i % 3 == 0)
                    o = new IndexedObject(i * 2);
                else if (i % 2 == 0)
                    o = new IndexedObject(i);
                else
                    o = null;

                assertEquals(o, cache.get(i));
            }

            info(" --> ok");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If fail.
     */
    public void testWalLargeValue() throws Exception {
        try {
            IgniteEx ignite = startGrid(1);

            ignite.cluster().active(true);

            IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < 10_000; i++) {
                final byte[] data = new byte[i];

                Arrays.fill(data, (byte)i);

                cache.put(i, data);

                if (i % 1000 == 0)
                    X.println(" ---> put: " + i);
            }

            stopGrid(1);

            ignite = startGrid(1);

            ignite.cluster().active(true);

            cache = ignite.cache(CACHE_NAME);

            info(" --> check2");

            for (int i = 0; i < 10_000; i++) {
                final byte[] data = new byte[i];

                Arrays.fill(data, (byte)i);

                final byte[] loaded = (byte[]) cache.get(i);

                Assert.assertArrayEquals(data, loaded);

                if (i % 1000 == 0)
                    X.println(" ---> get: " + i);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testWalRolloverMultithreadedDefault() throws Exception {
        logOnly = false;

        checkWalRolloverMultithreaded();
    }

    /**
     * @throws Exception if failed.
     */
    public void testWalRolloverMultithreadedLogOnly() throws Exception {
        logOnly = true;

        checkWalRolloverMultithreaded();
    }

    /**
     * @throws Exception if failed.
     */
    public void testHugeCheckpointRecord() throws Exception {
        long prevFDTimeout = customFailureDetectionTimeout;

        try {
            customFailureDetectionTimeout = 40_000;

            final IgniteEx ignite = startGrid(1);

            ignite.cluster().active(true);

            for (int i = 0; i < 50; i++) {
                CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("cache-" + i);

                // We can get 'too many open files' with default number of partitions.
                ccfg.setAffinity(new RendezvousAffinityFunction(false, 128));

                IgniteCache<Object, Object> cache = ignite.getOrCreateCache(ccfg);

                cache.put(i, i);
            }

            final long endTime = System.currentTimeMillis() + 30_000;

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() {
                    Random rnd = ThreadLocalRandom.current();

                    while (U.currentTimeMillis() < endTime) {
                        IgniteCache<Object, Object> cache = ignite.cache("cache-" + rnd.nextInt(50));

                        cache.put(rnd.nextInt(50_000), rnd.nextInt());
                    }

                    return null;
                }
            }, 16, "put-thread");

            while (System.currentTimeMillis() < endTime) {
                ignite.context().cache().context().database().wakeupForCheckpoint("test").get();

                U.sleep(500);
            }

            fut.get();
        }
        finally {
            customFailureDetectionTimeout = prevFDTimeout;

            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    private void checkWalRolloverMultithreaded() throws Exception {
        walSegmentSize = 2 * 1024 * 1024;

        final long endTime = System.currentTimeMillis() + 60 * 1000;

        try {
            IgniteEx ignite = startGrid(1);

            ignite.cluster().active(true);

            final IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() {
                    Random rnd = ThreadLocalRandom.current();

                    while (U.currentTimeMillis() < endTime)
                        cache.put(rnd.nextInt(50_000), rnd.nextInt());

                    return null;
                }
            }, 16, "put-thread");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If fail.
     */
    public void testWalRenameDirSimple() throws Exception {
        try {
            IgniteEx ignite = startGrid(1);

            ignite.cluster().active(true);

            IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < 100; i++)
                cache.put(i, new IndexedObject(i));

            final Object consistentId = ignite.cluster().localNode().consistentId();

            stopGrid(1);

            final File cacheDir = cacheDir(CACHE_NAME, consistentId.toString());

            renamed = cacheDir.renameTo(new File(cacheDir.getParent(), "cache-" + RENAMED_CACHE_NAME));

            assert renamed;

            ignite = startGrid(1);

            ignite.cluster().active(true);

            cache = ignite.cache(RENAMED_CACHE_NAME);

            for (int i = 0; i < 100; i++)
                assertEquals(new IndexedObject(i), cache.get(i));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param consId Consistent ID.
     * @return Cache dir.
     * @throws IgniteCheckedException If fail.
     */
    private File cacheDir(final String cacheName, final String consId) throws IgniteCheckedException {
        final String subfolderName
            = PdsConsistentIdProcessor.genNewStyleSubfolderName(0, UUID.fromString(consId));

        final File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        assert dbDir.exists();

        final File consIdDir = new File(dbDir.getAbsolutePath(), subfolderName);

        assert consIdDir.exists();

        final File cacheDir = new File(consIdDir.getAbsolutePath(), "cache-" + cacheName);

        assert cacheDir.exists();

        return cacheDir;
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryNoCheckpoint() throws Exception {
        try {
            IgniteEx ctrlGrid = startGrid(0);

            fork = true;

            IgniteEx cacheGrid = startGrid(1);

            ctrlGrid.cluster().active(true);

            ctrlGrid.compute(ctrlGrid.cluster().forRemotes()).run(new LoadRunnable(false));

            info("Killing remote process...");

            ((IgniteProcessProxy)cacheGrid).kill();

            final IgniteEx g0 = ctrlGrid;

            GridTestUtils.waitForCondition(new PA() {
                /** {@inheritDoc} */
                @Override public boolean apply() {
                    return g0.cluster().nodes().size() == 1;
                }
            }, getTestTimeout());

            fork = false;

            // Now start the grid and verify that updates were restored from WAL.
            cacheGrid = startGrid(1);

            IgniteCache<Object, Object> cache = cacheGrid.cache(CACHE_NAME);

            for (int i = 0; i < 10_000; i++)
                assertEquals(new IndexedObject(i), cache.get(i));

            List<List<?>> res = cache.query(new SqlFieldsQuery("select count(iVal) from IndexedObject")).getAll();

            assertEquals(1, res.size());
            assertEquals(10_000L, res.get(0).get(0));

            IgniteCache<Object, Object> locCache = cacheGrid.cache(LOC_CACHE_NAME);

            for (int i = 0; i < 10_000; i++)
                assertEquals(new IndexedObject(i), locCache.get(i));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testRecoveryLargeNoCheckpoint() throws Exception {
        try {
            IgniteEx ctrlGrid = startGrid(0);

            fork = true;

            IgniteEx cacheGrid = startGrid(1);

            ctrlGrid.cluster().active(true);

            ctrlGrid.compute(ctrlGrid.cluster().forRemotes()).run(new LargeLoadRunnable(false));

            info("Killing remote process...");

            ((IgniteProcessProxy)cacheGrid).kill();

            final IgniteEx g0 = ctrlGrid;

            GridTestUtils.waitForCondition(new PA() {
                /** {@inheritDoc} */
                @Override public boolean apply() {
                    return g0.cluster().nodes().size() == 1;
                }
            }, getTestTimeout());

            fork = false;

            // Now start the grid and verify that updates were restored from WAL.
            cacheGrid = startGrid(1);

            IgniteCache<Object, Object> cache = cacheGrid.cache(CACHE_NAME);
            IgniteCache<Object, Object> locCache = cacheGrid.cache(LOC_CACHE_NAME);

            for (int i = 0; i < 1000; i++) {
                final long[] data = new long[LARGE_ARR_SIZE];

                Arrays.fill(data, i);

                Assert.assertArrayEquals(data, (long[])cache.get(i));
                Assert.assertArrayEquals(data, (long[])locCache.get(i));
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
     * @throws Exception if failed.
     */
    public void testRandomCrash() throws Exception {
        try {
            IgniteEx ctrlGrid = startGrid(0);

            fork = true;

            IgniteEx cacheGrid = startGrid(1);

            ctrlGrid.cluster().active(true);

            IgniteCompute rmt = ctrlGrid.compute(ctrlGrid.cluster().forRemotes());

            rmt.run(new LoadRunnable(false));

            info(">>> Finished cache population.");

            rmt.run(new AsyncLoadRunnable());

            Thread.sleep(20_000);

            info(">>> Killing remote process...");

            ((IgniteProcessProxy)cacheGrid).kill();

            startGrid(1);

            Boolean res = rmt.call(new VerifyCallable());

            assertTrue(res);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testLargeRandomCrash() throws Exception {
        try {
            IgniteEx ctrlGrid = startGrid(0);

            fork = true;

            IgniteEx cacheGrid = startGrid(1);

            ctrlGrid.cluster().active(true);

            IgniteCompute rmt = ctrlGrid.compute(ctrlGrid.cluster().forRemotes());

            rmt.run(new LargeLoadRunnable(false));

            info(">>> Finished cache population.");

            rmt.run(new AsyncLargeLoadRunnable());

            Thread.sleep(20_000);

            info(">>> Killing remote process...");

            ((IgniteProcessProxy)cacheGrid).kill();

            startGrid(1);

            Boolean res = rmt.call(new VerifyLargeCallable());

            assertTrue(res);
        }
        finally {
            stopAllGrids();
        }
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
     * @throws Exception If failed.
     */
    public void testDestroyCache() throws Exception {
        try {
            IgniteEx ignite = startGrid(1);

            ignite.cluster().active(true);

            IgniteCache<Object, Object> cache = ignite.getOrCreateCache("test");

            cache.put(1, new IndexedObject(1));

            ignite.destroyCache("test");

            cache = ignite.getOrCreateCache("test");

            // No entry available after cache destroy.
            assertNull(cache.get(1));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If fail.
     */
    public void testEvictPartition() throws Exception {
        try {
            Ignite ignite1 = startGrid("node1");

            ignite1.cluster().active(true);

            IgniteCache<Object, Object> cache1 = ignite1.cache(CACHE_NAME);

            for (int i = 0; i < 100; i++)
                cache1.put(i, new IndexedObject(i));

            Ignite ignite2 = startGrid("node2");

            IgniteCache<Object, Object> cache2 = ignite2.cache(CACHE_NAME);

            for (int i = 0; i < 100; i++) {
                assertEquals(new IndexedObject(i), cache1.get(i));
                assertEquals(new IndexedObject(i), cache2.get(i));
            }

            ignite1.close();
            ignite2.close();

            ignite1 = startGrid("node1");
            ignite2 = startGrid("node2");

            ignite1.cluster().active(true);

            cache1 = ignite1.cache(CACHE_NAME);
            cache2 = ignite2.cache(CACHE_NAME);

            for (int i = 0; i < 100; i++) {
                assertEquals(new IndexedObject(i), cache1.get(i));
                assertEquals(new IndexedObject(i), cache2.get(i));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If fail.
     */
    public void testMetastorage() throws Exception {
        try {
            int cnt = 5000;

            IgniteEx ignite0 = (IgniteEx)startGrid("node1");
            IgniteEx ignite1 = (IgniteEx)startGrid("node2");

            ignite1.cluster().active(true);

            GridCacheSharedContext<Object, Object> sharedCtx0 = ignite0.context().cache().context();
            GridCacheSharedContext<Object, Object> sharedCtx1 = ignite1.context().cache().context();

            MetaStorage storage0 = sharedCtx0.database().metaStorage();
            MetaStorage storage1 = sharedCtx1.database().metaStorage();

            assert storage0 != null;

            for (int i = 0; i < cnt; i++) {
                sharedCtx0.database().checkpointReadLock();

                try {
                    storage0.putData(String.valueOf(i), new byte[]{(byte)(i % 256), 2, 3});
                }
                finally {
                    sharedCtx0.database().checkpointReadUnlock();
                }

                byte[] b1 = new byte[i + 3];
                b1[0] = 1;
                b1[1] = 2;
                b1[2] = 3;

                sharedCtx1.database().checkpointReadLock();

                try {
                    storage1.putData(String.valueOf(i), b1);
                }
                finally {
                    sharedCtx1.database().checkpointReadUnlock();
                }
            }

            for (int i = 0; i < cnt; i++) {
                byte[] d1 = storage0.getData(String.valueOf(i));
                assertEquals(3, d1.length);
                assertEquals((byte)(i % 256), d1[0]);
                assertEquals(2, d1[1]);
                assertEquals(3, d1[2]);

                byte[] d2 = storage1.getData(String.valueOf(i));
                assertEquals(i + 3, d2.length);
                assertEquals(1, d2[0]);
                assertEquals(2, d2[1]);
                assertEquals(3, d2[2]);
            }

        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If fail.
     */
    public void testMetastorageLargeArray() throws Exception {
        try {
            int cnt = 5000;
            int arraySize = 32_768;

            IgniteEx ignite = (IgniteEx)startGrid("node1");

            ignite.cluster().active(true);

            GridCacheSharedContext<Object, Object> sharedCtx = ignite.context().cache().context();

            MetaStorage storage = sharedCtx.database().metaStorage();

            for (int i = 0; i < cnt; i++) {
                byte[] b1 = new byte[arraySize];
                for (int k = 0; k < arraySize; k++) {
                    b1[k] = (byte) (k % 100);
                }

                sharedCtx.database().checkpointReadLock();

                try {
                    storage.putData(String.valueOf(i), b1);
                }
                finally {
                    sharedCtx.database().checkpointReadUnlock();
                }
            }

            for (int i = 0; i < cnt; i++) {
                byte[] d2 = storage.getData(String.valueOf(i));
                assertEquals(arraySize, d2.length);

                for (int k = 0; k < arraySize; k++) {
                    assertEquals((byte) (k % 100), d2[k]);
                }
            }

        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If fail.
     */
    public void testMetastorageRemove() throws Exception {
        try {
            int cnt = 400;

            IgniteEx ignite0 = (IgniteEx)startGrid("node1");

            ignite0.cluster().active(true);

            GridCacheSharedContext<Object, Object> sharedCtx0 = ignite0.context().cache().context();

            MetaStorage storage = sharedCtx0.database().metaStorage();

            assert storage != null;

            for (int i = 0; i < cnt; i++) {
                sharedCtx0.database().checkpointReadLock();

                try {
                    storage.putData(String.valueOf(i), new byte[]{1, 2, 3});
                }
                finally {
                    sharedCtx0.database().checkpointReadUnlock();
                }
            }

            for (int i = 0; i < 10; i++) {
                sharedCtx0.database().checkpointReadLock();

                try {
                    storage.removeData(String.valueOf(i));
                }
                finally {
                    sharedCtx0.database().checkpointReadUnlock();
                }
            }

            for (int i = 10; i < cnt; i++) {
                byte[] d1 = storage.getData(String.valueOf(i));
                assertEquals(3, d1.length);
                assertEquals(1, d1[0]);
                assertEquals(2, d1[1]);
                assertEquals(3, d1[2]);
            }

        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If fail.
     */
    public void testMetastorageUpdate() throws Exception {
        try {
            int cnt = 2000;

            IgniteEx ignite0 = (IgniteEx)startGrid("node1");

            ignite0.cluster().active(true);

            GridCacheSharedContext<Object, Object> sharedCtx0 = ignite0.context().cache().context();

            MetaStorage storage = sharedCtx0.database().metaStorage();

            assert storage != null;

            for (int i = 0; i < cnt; i++) {
                sharedCtx0.database().checkpointReadLock();

                try {
                    storage.putData(String.valueOf(i), new byte[]{1, 2, 3});
                }
                finally {
                    sharedCtx0.database().checkpointReadUnlock();
                }
            }

            for (int i = 0; i < cnt; i++) {
                sharedCtx0.database().checkpointReadLock();

                try {
                    storage.putData(String.valueOf(i), new byte[]{2, 2, 3, 4});
                }
                finally {
                    sharedCtx0.database().checkpointReadUnlock();
                }
            }

            for (int i = 0; i < cnt; i++) {
                byte[] d1 = storage.getData(String.valueOf(i));
                assertEquals(4, d1.length);
                assertEquals(2, d1[0]);
                assertEquals(2, d1[1]);
                assertEquals(3, d1[2]);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If fail.
     */
    public void testMetastorageWalRestore() throws Exception {
        try {
            int cnt = 2000;

            IgniteEx ignite0 = startGrid(0);

            ignite0.cluster().active(true);

            GridCacheSharedContext<Object, Object> sharedCtx0 = ignite0.context().cache().context();

            MetaStorage storage = sharedCtx0.database().metaStorage();

            assert storage != null;

            for (int i = 0; i < cnt; i++) {
                sharedCtx0.database().checkpointReadLock();

                try {
                    storage.putData(String.valueOf(i), new byte[]{1, 2, 3});
                }
                finally {
                    sharedCtx0.database().checkpointReadUnlock();
                }
            }

            for (int i = 0; i < cnt; i++) {
                byte[] value = storage.getData(String.valueOf(i));
                assert value != null;
                assert value.length == 3;
            }

            stopGrid(0);

            ignite0 = startGrid(0);

            ignite0.cluster().active(true);

            sharedCtx0 = ignite0.context().cache().context();

            storage = sharedCtx0.database().metaStorage();

            assert storage != null;

            for (int i = 0; i < cnt; i++) {
                byte[] value = storage.getData(String.valueOf(i));
                assert value != null;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testApplyDeltaRecords() throws Exception {
        try {
            IgniteEx ignite0 = (IgniteEx)startGrid("node0");

            ignite0.cluster().active(true);

            IgniteCache<Object, Object> cache0 = ignite0.cache(CACHE_NAME);

            for (int i = 0; i < 1000; i++)
                cache0.put(i, new IndexedObject(i));

            GridCacheSharedContext<Object, Object> sharedCtx = ignite0.context().cache().context();

            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)sharedCtx.database();

            db.waitForCheckpoint("test");
            db.enableCheckpoints(false).get();

            // Log something to know where to start.
            WALPointer ptr = sharedCtx.wal().log(new MemoryRecoveryRecord(U.currentTimeMillis()));

            info("Replay marker: " + ptr);

            for (int i = 1000; i < 5000; i++)
                cache0.put(i, new IndexedObject(i));

            info("Done puts...");

            for (int i = 2_000; i < 3_000; i++)
                cache0.remove(i);

            info("Done removes...");

            for (int i = 5000; i < 6000; i++)
                cache0.put(i, new IndexedObject(i));

            info("Done puts...");

            Map<FullPageId, byte[]> rolledPages = new HashMap<>();

            int pageSize = sharedCtx.database().pageSize();

            ByteBuffer buf = ByteBuffer.allocateDirect(pageSize);

            // Now check that deltas can be correctly applied.
            try (WALIterator it = sharedCtx.wal().replay(ptr)) {
                while (it.hasNext()) {
                    IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                    WALRecord rec = tup.get2();

                    if (rec instanceof PageSnapshot) {
                        PageSnapshot page = (PageSnapshot)rec;

                        rolledPages.put(page.fullPageId(), page.pageData());
                    }
                    else if (rec instanceof PageDeltaRecord) {
                        PageDeltaRecord delta = (PageDeltaRecord)rec;

                        FullPageId fullId = new FullPageId(delta.pageId(), delta.groupId());

                        byte[] pageData = rolledPages.get(fullId);

                        if (pageData == null) {
                            pageData = new byte[pageSize];

                            rolledPages.put(fullId, pageData);
                        }

                        assertNotNull("Missing page snapshot [page=" + fullId + ", delta=" + delta + ']', pageData);

                        buf.order(ByteOrder.nativeOrder());

                        buf.position(0);
                        buf.put(pageData);
                        buf.position(0);

                        delta.applyDelta(sharedCtx.database().dataRegion(null).pageMemory(),
                            GridUnsafe.bufferAddress(buf));

                        buf.position(0);

                        buf.get(pageData);
                    }
                }
            }

            info("Done apply...");

            PageMemoryEx pageMem = (PageMemoryEx)db.dataRegion(null).pageMemory();

            for (Map.Entry<FullPageId, byte[]> entry : rolledPages.entrySet()) {
                FullPageId fullId = entry.getKey();

                ignite0.context().cache().context().database().checkpointReadLock();

                try {
                    long page = pageMem.acquirePage(fullId.groupId(), fullId.pageId(), true);

                    try {
                        long bufPtr = pageMem.writeLock(fullId.groupId(), fullId.pageId(), page, true);

                        try {
                            byte[] data = entry.getValue();

                            for (int i = 0; i < data.length; i++) {
                                if (fullId.pageId() == TrackingPageIO.VERSIONS.latest().trackingPageFor(fullId.pageId(), db.pageSize()))
                                    continue; // Skip tracking pages.

                                assertEquals("page=" + fullId + ", pos=" + i, PageUtils.getByte(bufPtr, i), data[i]);
                            }
                        }
                        finally {
                            pageMem.writeUnlock(fullId.groupId(), fullId.pageId(), page, null, false, true);
                        }
                    }
                    finally {
                        pageMem.releasePage(fullId.groupId(), fullId.pageId(), page);
                    }
                }
                finally {
                    ignite0.context().cache().context().database().checkpointReadUnlock();
                }
            }

            ignite0.close();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test recovery from WAL on 3 nodes in case of transactional cache.
     *
     * @throws Exception If fail.
     */
    public void testRecoveryOnTransactionalAndPartitionedCache() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrids(3);
        ignite.cluster().active(true);

        try {
            final String cacheName = "transactional";

            CacheConfiguration<Object, Object> cacheConfiguration = new CacheConfiguration<>(cacheName)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setRebalanceMode(CacheRebalanceMode.SYNC)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                    .setBackups(2);

            ignite.createCache(cacheConfiguration);

            IgniteCache<Object, Object> cache = ignite.cache(cacheName);
            Map<Object, Object> map = new HashMap<>();

            final int transactions = 100;
            final int operationsPerTransaction = 40;

            Random random = new Random();

            for (int t = 1; t <= transactions; t++) {
                Transaction tx = ignite.transactions().txStart(
                        TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                Map<Object, Object> changesInTransaction = new HashMap<>();

                for (int op = 0; op < operationsPerTransaction; op++) {
                    int key = random.nextInt(1000) + 1;

                    Object value = random.nextBoolean() ? randomString(random) + key : new BigObject(key);

                    changesInTransaction.put(key, value);

                    cache.put(key, value);
                }

                if (random.nextBoolean()) {
                    tx.commit();
                    map.putAll(changesInTransaction);
                }
                else {
                    tx.rollback();
                }

                if (t % 50 == 0)
                    log.info("Finished transaction " + t);
            }

            stopAllGrids();

            ignite = (IgniteEx) startGrids(3);
            ignite.cluster().active(true);

            cache = ignite.cache(cacheName);

            for (Object key : map.keySet()) {
                Object expectedValue = map.get(key);
                Object actualValue = cache.get(key);
                Assert.assertEquals("Unexpected value for key " + key, expectedValue, actualValue);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test that all DataRecord WAL records are within transaction boundaries - PREPARED and COMMITTED markers.
     *
     * @throws Exception If any fail.
     */
    public void testTxRecordsConsistency() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_WAL_LOG_TX_RECORDS, "true");

        IgniteEx ignite = (IgniteEx) startGrids(3);
        ignite.cluster().active(true);

        try {
            final String cacheName = "transactional";

            CacheConfiguration<Object, Object> cacheConfiguration = new CacheConfiguration<>(cacheName)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                    .setAffinity(new RendezvousAffinityFunction(false, 32))
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setRebalanceMode(CacheRebalanceMode.SYNC)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                    .setBackups(0);

            ignite.createCache(cacheConfiguration);

            IgniteCache<Object, Object> cache = ignite.cache(cacheName);

            GridCacheSharedContext<Object, Object> sharedCtx = ignite.context().cache().context();

            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)sharedCtx.database();

            db.waitForCheckpoint("test");
            db.enableCheckpoints(false).get();

            // Log something to know where to start.
            WALPointer startPtr = sharedCtx.wal().log(new MemoryRecoveryRecord(U.currentTimeMillis()));

            final int transactions = 100;
            final int operationsPerTransaction = 40;

            Random random = new Random();

            for (int t = 1; t <= transactions; t++) {
                Transaction tx = ignite.transactions().txStart(
                        TransactionConcurrency.OPTIMISTIC, TransactionIsolation.READ_COMMITTED);

                for (int op = 0; op < operationsPerTransaction; op++) {
                    int key = random.nextInt(1000) + 1;

                    Object value = random.nextBoolean() ? randomString(random) + key : new BigObject(key);

                    cache.put(key, value);
                }

                if (random.nextBoolean()) {
                    tx.commit();
                }
                else {
                    tx.rollback();
                }

                if (t % 50 == 0)
                    log.info("Finished transaction " + t);
            }

            Set<GridCacheVersion> activeTransactions = new HashSet<>();

            // Check that all DataRecords are within PREPARED and COMMITTED tx records.
            try (WALIterator it = sharedCtx.wal().replay(startPtr)) {
                while (it.hasNext()) {
                    IgniteBiTuple<WALPointer, WALRecord> tup = it.next();

                    WALRecord rec = tup.get2();

                    if (rec instanceof TxRecord) {
                        TxRecord txRecord = (TxRecord) rec;
                        GridCacheVersion txId = txRecord.nearXidVersion();

                        switch (txRecord.state()) {
                            case PREPARED:
                                assert !activeTransactions.contains(txId) : "Transaction is already present " + txRecord;

                                activeTransactions.add(txId);

                                break;
                            case COMMITTED:
                                assert activeTransactions.contains(txId) : "No PREPARE marker for transaction " + txRecord;

                                activeTransactions.remove(txId);

                                break;
                            case ROLLED_BACK:
                                activeTransactions.remove(txId);
                                break;

                            default:
                                throw new IllegalStateException("Unknown Tx state of record " + txRecord);
                        }
                    } else if (rec instanceof DataRecord) {
                        DataRecord dataRecord = (DataRecord) rec;

                        for (DataEntry entry : dataRecord.writeEntries()) {
                            GridCacheVersion txId = entry.nearXidVersion();

                            assert activeTransactions.contains(txId) : "No transaction for entry " + entry;
                        }
                    }
                }
            }
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_WAL_LOG_TX_RECORDS);
            stopAllGrids();
        }
    }

    /**
     * Generate random lowercase string for test purposes.
     */
    private String randomString(Random random) {
        int len = random.nextInt(50) + 1;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++)
            sb.append(random.nextInt(26) + 'a');

        return sb.toString();
    }

    /**
     * BigObject for test purposes that don't fit in page size.
     */
    private static class BigObject {
        private final int index;

        private final byte[] payload = new byte[4096];

        BigObject(int index) {
            this.index = index;
            // Create pseudo-random array.
            for (int i = 0; i < payload.length; i++)
                if (i % index == 0)
                    payload[i] = (byte) index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BigObject bigObject = (BigObject) o;
            return index == bigObject.index &&
                    Arrays.equals(payload, bigObject.payload);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, payload);
        }
    }

    /**
     * @param size Size of data.
     * @return Test data.
     */
    private int[] createTestData(int size) {
        int[] data = new int[size];

        for (int d = 0; d < size; ++d)
            data[d] = d;

        return data;
    }

    /**
     *
     */
    private static class LoadRunnable implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private boolean disableCheckpoints;

        /**
         * @param disableCheckpoints Disable checkpoints flag.
         */
        private LoadRunnable(boolean disableCheckpoints) {
            this.disableCheckpoints = disableCheckpoints;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ignite.log().info("Started load.");

            if (disableCheckpoints) {
                GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                    .cache().context().database();

                try {
                    dbMgr.enableCheckpoints(false).get();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }

            try {
                boolean successfulWaiting = GridTestUtils.waitForCondition(new PAX() {
                    @Override public boolean applyx() {
                        return ignite.cache(CACHE_NAME) != null;
                    }
                }, 10_000);

                assertTrue(successfulWaiting);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }

            IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);
            IgniteCache<Object, Object> locCache = ignite.cache(LOC_CACHE_NAME);

            for (int i = 0; i < 10_000; i++) {
                cache.put(i, new IndexedObject(i));
                locCache.put(i, new IndexedObject(i));
            }

            ignite.log().info("Finished load.");
        }
    }

    /**
     *
     */
    private static class AsyncLoadRunnable implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                boolean successfulWaiting = GridTestUtils.waitForCondition(new PAX() {
                    @Override public boolean applyx() {
                        return ignite.cache(CACHE_NAME) != null;
                    }
                }, 10_000);

                assertTrue(successfulWaiting);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }

            ignite.log().info(">>>>>>> Started load.");

            for (int i = 0; i < 4; i++) {
                ignite.scheduler().callLocal(new Callable<Object>() {
                    @Override public Object call() {
                        IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);
                        IgniteCache<Object, Object> locCache = ignite.cache(LOC_CACHE_NAME);

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        int cnt = 0;

                        while (!Thread.currentThread().isInterrupted()) {
                            cache.put(rnd.nextInt(10_000), new IndexedObject(rnd.nextInt()));
                            locCache.put(rnd.nextInt(10_000), new IndexedObject(rnd.nextInt()));

                            cnt++;

                            if (cnt > 0 && cnt % 1_000 == 0)
                                ignite.log().info(">>>> Updated: " + cnt);
                        }

                        return null;
                    }
                });
            }
        }
    }

    /**
     *
     */
    private static class VerifyCallable implements IgniteCallable<Boolean> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            try {
                boolean successfulWaiting = GridTestUtils.waitForCondition(new PAX() {
                    @Override public boolean applyx() {
                        return ignite.cache(CACHE_NAME) != null;
                    }
                }, 10_000);

                assertTrue(successfulWaiting);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }

            IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);
            IgniteCache<Object, Object> locCache = ignite.cache(LOC_CACHE_NAME);

            for (int i = 0; i < 10_000; i++) {
                {
                    Object val = cache.get(i);

                    if (val == null) {
                        ignite.log().warning("Failed to find a value for PARTITIONED cache key: " + i);

                        return false;
                    }
                }

                {
                    Object val = locCache.get(i);

                    if (val == null) {
                        ignite.log().warning("Failed to find a value for LOCAL cache key: " + i);

                        return false;
                    }
                }
            }

            return true;
        }
    }

    /**
     *
     */
    private static class LargeLoadRunnable implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private boolean disableCheckpoints;

        /**
         * @param disableCheckpoints Disable checkpoints flag.
         */
        private LargeLoadRunnable(boolean disableCheckpoints) {
            this.disableCheckpoints = disableCheckpoints;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                boolean successfulWaiting = GridTestUtils.waitForCondition(new PAX() {
                    @Override public boolean applyx() {
                        return ignite.cache(CACHE_NAME) != null;
                    }
                }, 10_000);

                assertTrue(successfulWaiting);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }

            ignite.log().info("Started load.");

            if (disableCheckpoints) {
                GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                    .cache().context().database();

                dbMgr.enableCheckpoints(false);
            }

            IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);
            IgniteCache<Object, Object> locCache = ignite.cache(LOC_CACHE_NAME);

            for (int i = 0; i < 1000; i++) {
                final long[] data = new long[LARGE_ARR_SIZE];

                Arrays.fill(data, i);

                cache.put(i, data);
                locCache.put(i, data);
            }

            ignite.log().info("Finished load.");
        }
    }

    /**
     *
     */
    private static class AsyncLargeLoadRunnable implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                boolean successfulWaiting = GridTestUtils.waitForCondition(new PAX() {
                    @Override public boolean applyx() {
                        return ignite.cache(CACHE_NAME) != null;
                    }
                }, 10_000);

                assertTrue(successfulWaiting);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }

            ignite.log().info(">>>>>>> Started load.");

            for (int i = 0; i < 1; i++) {
                ignite.scheduler().callLocal(new Callable<Object>() {
                    @Override public Object call() {
                        IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        int cnt = 0;

                        while (!Thread.currentThread().isInterrupted()) {
                            final long[] data = new long[LARGE_ARR_SIZE];

                            final int key = rnd.nextInt(1000);

                            Arrays.fill(data, key);

                            cache.put(key, data);

                            cnt++;

                            if (cnt > 0 && cnt % 1_000 == 0)
                                ignite.log().info(">>>> Updated: " + cnt);
                        }

                        return null;
                    }
                });
            }
        }
    }

    /**
     *
     */
    private static class VerifyLargeCallable implements IgniteCallable<Boolean> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            try {
                boolean successfulWaiting = GridTestUtils.waitForCondition(new PAX() {
                    @Override public boolean applyx() {
                        return ignite.cache(CACHE_NAME) != null;
                    }
                }, 10_000);

                assertTrue(successfulWaiting);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }

            IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < 1000; i++) {
                final long[] data = new long[LARGE_ARR_SIZE];

                Arrays.fill(data, i);

                final Object val = cache.get(i);

                if (val == null) {
                    ignite.log().warning("Failed to find a value for key: " + i);

                    return false;
                }

                assertTrue(Arrays.equals(data, (long[])val));
            }

            return true;
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

    /**
     *
     */
    private enum EnumVal {
        /** */
        VAL1,

        /** */
        VAL2,

        /** */
        VAL3
    }
}
