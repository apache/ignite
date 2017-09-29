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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
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
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
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
import sun.nio.ch.DirectBuffer;

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
    private String cacheName;

    /** */
    private int walSegmentSize;

    /** Logger only. */
    private boolean logOnly;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return fork;
    }

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

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        dbCfg.setPageSize(4 * 1024);

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setInitialSize(1024 * 1024 * 1024);
        memPlcCfg.setMaxSize(1024 * 1024 * 1024);

        dbCfg.setMemoryPolicies(memPlcCfg);
        dbCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(dbCfg);

        PersistentStoreConfiguration pCfg = new PersistentStoreConfiguration();

        pCfg.setWalRecordIteratorBufferSize(1024 * 1024);

        pCfg.setWalHistorySize(2);

        if (logOnly)
            pCfg.setWalMode(WALMode.LOG_ONLY);

        if (walSegmentSize != 0)
            pCfg.setWalSegmentSize(walSegmentSize);

        cfg.setPersistentStoreConfiguration(pCfg);

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

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));

        cacheName = "partitioned";
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        logOnly = false;

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /**
     * @throws Exception if failed.
     */
    public void testWalBig() throws Exception {
        IgniteEx ignite = startGrid(1);

        ignite.active(true);

        IgniteCache<Object, Object> cache = ignite.cache("partitioned");

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

        ignite.active(true);

        cache = ignite.cache("partitioned");

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

        ignite.active(true);

        IgniteCache<Object, Object> cache = ignite.cache("partitioned");

        for (int i = 0; i < MAX_SIZE_POWER; ++i) {
            int size = 1 << i;

            cache.put("key_" + i, createTestData(size));
        }

        stopGrid(1, true);

        ignite = startGrid(1);

        ignite.active(true);

        cache = ignite.cache("partitioned");

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

            igniteEx.active(true);

            IgniteCache<Integer, EnumVal> cache = igniteEx.cache("partitioned");

            // Creates LoadCacheJobV2
//            cache.loadCache(null);

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

            ignite.active(true);

            IgniteCache<Object, Object> cache = ignite.cache("partitioned");

            info(" --> step1");

            for (int i = 0; i < 10_000; i += 2) {
//                X.println(" -> put: " + i);

                cache.put(i, new IndexedObject(i));
            }

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

            ignite.active(true);

            cache = ignite.cache("partitioned");

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

            ignite.active(true);

            IgniteCache<Object, Object> cache = ignite.cache("partitioned");

            for (int i = 0; i < 10_000; i++) {
                final byte[] data = new byte[i];

                Arrays.fill(data, (byte)i);

                cache.put(i, data);

                if (i % 1000 == 0)
                    X.println(" ---> put: " + i);

//                Assert.assertArrayEquals(data, (byte[])cache.get(i));
            }

//            info(" --> check1");
//
//            for (int i = 0; i < 25_000; i++) {
//                final byte[] data = new byte[i];
//
//                Arrays.fill(data, (byte)i);
//
//                final byte[] loaded = (byte[]) cache.get(i);
//
//                Assert.assertArrayEquals(data, loaded);
//            }

            stopGrid(1);

            ignite = startGrid(1);

            ignite.active(true);

            cache = ignite.cache("partitioned");

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
        try {
            final IgniteEx ignite = startGrid(1);

            ignite.active(true);

            for (int i = 0; i < 50; i++) {
                CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("cache-" + i);

                // We can get 'too many open files' with default number of partitions.
                ccfg.setAffinity(new RendezvousAffinityFunction(false, 128));

                IgniteCache<Object, Object> cache = ignite.getOrCreateCache(ccfg);

                cache.put(i, i);
            }

            final long endTime = System.currentTimeMillis() + 30_000;

            IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
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
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    private void checkWalRolloverMultithreaded() throws Exception {
        walSegmentSize = 2 * 1024 * 1024;

        final long endTime = System.currentTimeMillis() + 2 * 60 * 1000;

        try {
            IgniteEx ignite = startGrid(1);

            ignite.active(true);

            final IgniteCache<Object, Object> cache = ignite.cache("partitioned");

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
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

            ignite.active(true);

            IgniteCache<Object, Object> cache = ignite.cache("partitioned");

            for (int i = 0; i < 100; i++)
                cache.put(i, new IndexedObject(i));

            stopGrid(1);

            final File cacheDir = cacheDir("partitioned", ignite.context().discovery().consistentId().toString());

            final boolean renamed = cacheDir.renameTo(new File(cacheDir.getParent(), "cache-partitioned0"));

            assert renamed;

            cacheName = "partitioned0";

            ignite = startGrid(1);

            ignite.active(true);

            cache = ignite.cache(cacheName);

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
    private File cacheDir(final String cacheName, String consId) throws IgniteCheckedException {
        consId = consId.replaceAll("[\\.:]", "_");

        final File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        assert dbDir.exists();

        final File consIdDir = new File(dbDir.getAbsolutePath(), consId);

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

            ctrlGrid.active(true);

            fork = true;

            IgniteEx cacheGrid = startGrid(1);

            ctrlGrid.active(true);

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

            IgniteCache<Object, Object> cache = cacheGrid.cache("partitioned");

            for (int i = 0; i < 10_000; i++)
                assertEquals(new IndexedObject(i), cache.get(i));

            List<List<?>> res = cache.query(new SqlFieldsQuery("select count(iVal) from IndexedObject")).getAll();

            assertEquals(1, res.size());
            assertEquals(10_000L, res.get(0).get(0));
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

            ctrlGrid.active(true);

            fork = true;

            IgniteEx cacheGrid = startGrid(1);

            ctrlGrid.active(true);

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

            IgniteCache<Object, Object> cache = cacheGrid.cache("partitioned");

            for (int i = 0; i < 1000; i++) {
                final long[] data = new long[LARGE_ARR_SIZE];

                Arrays.fill(data, i);

                final long[] loaded = (long[]) cache.get(i);

                Assert.assertArrayEquals(data, loaded);
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

            ctrlGrid.active(true);

            fork = true;

            IgniteEx cacheGrid = startGrid(1);

            ctrlGrid.active(true);

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

            ctrlGrid.active(true);

            fork = true;

            IgniteEx cacheGrid = startGrid(1);

            ctrlGrid.active(true);

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

            ignite.active(true);

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

            ignite1.active(true);

            IgniteCache<Object, Object> cache1 = ignite1.cache(cacheName);

            for (int i = 0; i < 100; i++)
                cache1.put(i, new IndexedObject(i));

            Ignite ignite2 = startGrid("node2");

            IgniteCache<Object, Object> cache2 = ignite2.cache(cacheName);

            for (int i = 0; i < 100; i++) {
                assertEquals(new IndexedObject(i), cache1.get(i));
                assertEquals(new IndexedObject(i), cache2.get(i));
            }

            ignite1.close();
            ignite2.close();

            ignite1 = startGrid("node1");
            ignite2 = startGrid("node2");

            ignite1.active(true);

            cache1 = ignite1.cache(cacheName);
            cache2 = ignite2.cache(cacheName);

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
     * @throws Exception if failed.
     */
    public void testApplyDeltaRecords() throws Exception {
        try {
            IgniteEx ignite0 = (IgniteEx)startGrid("node0");

            ignite0.active(true);

            IgniteCache<Object, Object> cache0 = ignite0.cache(cacheName);

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

            ByteBuffer buf1 = ByteBuffer.allocateDirect(pageSize);

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

                        buf1.order(ByteOrder.nativeOrder());

                        buf1.position(0);
                        buf1.put(pageData);
                        buf1.position(0);

                        delta.applyDelta(sharedCtx
                                .database()
                                .memoryPolicy(null)
                                .pageMemory(),

                                ((DirectBuffer)buf1).address());

                        buf1.position(0);

                        buf1.get(pageData);
                    }
                }
            }

            info("Done apply...");

            PageMemoryEx pageMem = (PageMemoryEx)db.memoryPolicy(null).pageMemory();

            for (Map.Entry<FullPageId, byte[]> entry : rolledPages.entrySet()) {
                FullPageId fullId = entry.getKey();

                ignite0.context().cache().context().database().checkpointReadLock();

                try {
                    long page = pageMem.acquirePage(fullId.groupId(), fullId.pageId(), true);

                    try {
                        long buf = pageMem.writeLock(fullId.groupId(), fullId.pageId(), page, true);

                        try {
                            byte[] data = entry.getValue();

                            for (int i = 0; i < data.length; i++) {
                                if (fullId.pageId() == TrackingPageIO.VERSIONS.latest().trackingPageFor(fullId.pageId(), db.pageSize()))
                                    continue; // Skip tracking pages.

                                assertEquals("page=" + fullId + ", pos=" + i, PageUtils.getByte(buf, i), data[i]);
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
        ignite.active(true);

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

                    Object value;
                    if (random.nextBoolean())
                        value = randomString(random) + key;
                    else
                        value = new BigObject(key);

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
            ignite.active(true);

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
        ignite.active(true);

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

                    Object value;
                    if (random.nextBoolean())
                        value = randomString(random) + key;
                    else
                        value = new BigObject(key);

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
                        return ignite.cache("partitioned") != null;
                    }
                }, 10_000);

                assertTrue(successfulWaiting);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }

            IgniteCache<Object, Object> cache = ignite.cache("partitioned");

            for (int i = 0; i < 10_000; i++)
                cache.put(i, new IndexedObject(i));

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
                        return ignite.cache("partitioned") != null;
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
                    @Override public Object call() throws Exception {
                        IgniteCache<Object, Object> cache = ignite.cache("partitioned");

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        int cnt = 0;

                        while (!Thread.currentThread().isInterrupted()) {
                            cache.put(rnd.nextInt(10_000), new IndexedObject(rnd.nextInt()));

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
                        return ignite.cache("partitioned") != null;
                    }
                }, 10_000);

                assertTrue(successfulWaiting);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }

            IgniteCache<Object, Object> cache = ignite.cache("partitioned");

            for (int i = 0; i < 10_000; i++) {
                Object val = cache.get(i);

                if (val == null) {
                    ignite.log().warning("Failed to find a value for key: " + i);

                    return false;
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
                        return ignite.cache("partitioned") != null;
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

            IgniteCache<Object, Object> cache = ignite.cache("partitioned");

            for (int i = 0; i < 1000; i++) {
                final long[] data = new long[LARGE_ARR_SIZE];

                Arrays.fill(data, i);

                cache.put(i, data);
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
                        return ignite.cache("partitioned") != null;
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
                    @Override public Object call() throws Exception {
                        IgniteCache<Object, Object> cache = ignite.cache("partitioned");

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        int cnt = 0;

                        while (!Thread.currentThread().isInterrupted()) {
                            final long[] data = new long[LARGE_ARR_SIZE];

                            final int key = rnd.nextInt(1000);

                            Arrays.fill(data, key);

//                            System.out.println("> " + key);

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
                        return ignite.cache("partitioned") != null;
                    }
                }, 10_000);

                assertTrue(successfulWaiting);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new RuntimeException(e);
            }

            IgniteCache<Object, Object> cache = ignite.cache("partitioned");

            for (int i = 0; i < 1000; i++) {
                final long[] data = new long[LARGE_ARR_SIZE];

                Arrays.fill(data, i);

                final Object val = cache.get(i);

                if (val == null) {
                    ignite.log().warning("Failed to find a value for key: " + i);

                    return false;
                }
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
