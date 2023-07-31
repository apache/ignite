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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import org.apache.commons.lang3.RandomUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.DefragmentationMXBean;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests for defragmentation JMX bean.
 */
public class DefragmentationMXBeanTest extends GridCommonAbstractTest {
    /** */
    private static final int ITERATIONS = 1;

    /** */
    private static final int MIN_DATA_LEN = 40;

    /** */
    private static final int MAX_DATA_LEN = 4000;

    /** */
    private static final int PAGE_SIZE = 1024;

    /** */
    private static final int CACHE_PARTS_CNT = 16348;
//    private static final int CACHE_PARTS_CNT = 1024;

    /** */
    private static final int GEN_DATA_BUCKET_LEN = 10_000;

    /** */
    private static CountDownLatch blockCdl;

    /** */
    private static CountDownLatch waitCdl;

    /** */
    private WALMode walMode = WALMode.LOG_ONLY;

    /** */
    private boolean loadRemote;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        forceCheckpoint();

        stopAllGrids(false);

        super.afterTest();
    }

    /** */
    @Override protected long getTestTimeout() {
        return 600 * 60 * 10000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        final DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setWalMode(walMode);
//        dsCfg.setWalMode(WALMode.LOG_ONLY);

        dsCfg.setPageSize(PAGE_SIZE);

        if (loadRemote) {
            String localAddr = "10.111.149.249";

            TcpDiscoverySpi tcpDiscoverySpi = new TcpDiscoverySpi();

            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
            ipFinder.setAddresses(Collections.singletonList("10.40.6.208:47700..47700"));
            tcpDiscoverySpi.setIpFinder(ipFinder);
            tcpDiscoverySpi.setLocalPort(47700);
            tcpDiscoverySpi.setLocalPortRange(9);
            tcpDiscoverySpi.setLocalAddress(localAddr);

            cfg.setDiscoverySpi(tcpDiscoverySpi);

            TcpCommunicationSpi cm = new TcpCommunicationSpi();
            cm.setLocalPort(47900);
            cm.setLocalPortRange(9);
            cm.setLocalAddress(localAddr);

            tcpDiscoverySpi.setJoinTimeout(30000);

            cfg.setCommunicationSpi(cm);

//            cfg.setDataStreamerThreadPoolSize(2);
        } else {
//            cfg.setDataStreamerThreadPoolSize(1);

            cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));

            cfg.setCacheConfiguration(
                cacheCfg(0)
            );
        }

        cfg.setConsistentId("defragTestNode" + getTestIgniteInstanceIndex(igniteInstanceName));

        cfg.setMetricsLogFrequency(0);

        dsCfg.setWalSegmentSize(32 * 1024 * 1024);
        dsCfg.setMaxWalArchiveSize(1L * 1024L * 1024L * 1024L);
        dsCfg.setWalSegments(20);

        long regSize = 400L * 1024 * 1024;

        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setName("default")
                .setInitialSize(regSize).setMaxSize(regSize)
//                .setMaxSize(regSize)
//                .setPageReplacementMode(PageReplacementMode.CLOCK)
                .setPersistenceEnabled(true)
                .setMetricsEnabled(false)
        );

        String basePath = "/home/fmj/tmp/ignite/";

        dsCfg.setStoragePath(basePath + "storage");
        cfg.setWorkDirectory(basePath + "work");
        cfg.setSnapshotPath(basePath + "snapshot");
        dsCfg.setWalArchivePath(basePath + "wal_archive");
        dsCfg.setWalPath(basePath + "wal");
//        dsCfg.setWalCompactionEnabled(true);

//        dsCfg.setDefragmentationThreadPoolSize(1);
//        dsCfg.setCheckpointThreads(2);

        dsCfg.setCheckpointReadLockTimeout(280_000);
        dsCfg.setCheckpointFrequency(1500);

        return cfg.setDataStorageConfiguration(dsCfg);
    }

    @Test
    @WithSystemProperty(key = "IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK", value = "true")
    public void testLoadRemote() throws Exception {
        int cnt = 100_000_000;
//        int cnt = 10_000;

        loadRemote = true;

        Ignite client = startClientGrid(0);

        assert client.cluster().nodes().size() > 1;

        load(client, cnt, false);
    }

    /** */
    private static byte[][] data() throws IgniteCheckedException {
        byte[][] data = new byte[GEN_DATA_BUCKET_LEN][];

        AtomicInteger idx = new AtomicInteger();

        runAsync(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            int idx0;

            while ((idx0 = idx.getAndIncrement()) < GEN_DATA_BUCKET_LEN)
                data[idx0] = RandomUtils.nextBytes(MIN_DATA_LEN + rnd.nextInt(MAX_DATA_LEN - MIN_DATA_LEN));
        }).get();

        return data;
    }

    /**
     * Test that defragmentation won't be scheduled second time, if previously scheduled via maintenance registry.
     * Description:
     * 1. Start two nodes.
     * 2. Register defragmentation maintenance task on the first node.
     * 3. Restart node.
     * 3. Scheduling of the defragmentation on the first node via JMX bean should fail.
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_DUMP_THREADS_ON_FAILURE", value = "false")
    @WithSystemProperty(key = "IGNITE_SYSTEM_WORKER_BLOCKED_TIMEOUT", value = "300000")
//    @WithSystemProperty(key = "IGNITE_DEFRAGMENTATION_CP_THRESHOLD", value = "300")
//    @WithSystemProperty(key = "IGNITE_PAGES_LIST_DISABLE_ONHEAP_CACHING", value = "true")
    public void test0Defrag() throws Exception {
//        int cnt = 105_000_000;
//        int cnt = 80_000_000;
//       int cnt = 42_000_000;
        int cnt = 17_000_000;
//        int cnt = 500_000;

        IgniteEx ignite = startGrids(1);

        DefragmentationMXBean mxBean = defragmentationMXBean(ignite.name());

//        mxBean.cancel();

        if (!ignite.context().maintenanceRegistry().isMaintenanceMode()) {
            ignite.cluster().state(ACTIVE);

            log.info("Cache size: " + ignite.cache("cache0").size());

            if (ignite.cache("cache0").size() < cnt) {
                stopAllGrids(false);

                walMode = WALMode.NONE;

                ignite = startGrids(1);

                ignite.cluster().state(ACTIVE);

                load(ignite, cnt, true);

                forceCheckpoint(ignite);

                stopAllGrids(false);

                walMode = WALMode.LOG_ONLY;

                ignite = startGrids(1);

                ignite.cluster().state(ACTIVE);

                forceCheckpoint(ignite);
            }

            assertTrue(mxBean.schedule(""));

            stopAllGrids(false);

            ignite = startGrids(1);

            // node is already in defragmentation mode, hence scheduling is not possible
            assertFalse(mxBean.schedule(""));
        }

        waitForCondition(() -> !mxBean.inProgress(), getTestTimeout());
    }

    /** */
    private void load(Ignite node, int cnt, boolean rest) throws Exception {
        IgniteCache<Long, byte[]> cache = node.getOrCreateCache(cacheCfg(0));

        try (IgniteDataStreamer<Long, byte[]> ds = node.dataStreamer(cache.getName())) {
            ds.receiver(DataStreamerCacheUpdaters.batched());

            if (rest) {
                ds.perNodeParallelOperations(2);
                ds.perNodeBufferSize(32);
                ds.perThreadBufferSize(128);

                ds.autoFlushFrequency(3_000);
            }

            long size = cache.size();

            int iterations = ITERATIONS;

            Random rnd = new Random();

            long time = System.nanoTime();
            long partTime = time;

            for (int itr = 0; itr < iterations; ++itr) {
                byte[][] data = data();

                long dataSize = data.length;

                for (long i = size, info = size, step = cnt / 100; i < cnt; ++i) {
                    if (itr == 0)
                        ds.addData(i, data[(int)(i % dataSize)]);
                    else {
                        if (rnd.nextInt(100) > 80)
                            ds.removeData(i);
                        else
                            ds.addData(i, data[(int)(i % dataSize)]);
                    }

                    if (i - info >= step || U.nanosToMillis(System.nanoTime() - partTime) > 15_000) {
                        partTime = System.nanoTime();
                        info = i;

                        log.info(String.format(
                            "TEST | Loaded %d / %d records, (%.2f%%).",
//                            "TEST | Loaded %d / %d records, (%.2f%%). ETA: %.0f seconds.",
                            i,
                            cnt * iterations,
                            (double)(i + (itr * cnt)) / ((double)cnt * iterations) * 100
//                            ((double)U.nanosToMillis(System.nanoTime() - time) / (i - size)) * (cnt * iterations - i) / 1000
                        ));

                        if (rest) {
                            Thread.yield();
                            Thread.sleep(200);
                            Thread.yield();
                        }
                    }
                }
            }
        }

        log.info("TEST | Loaded completed.");

        Thread.sleep(3_000);
    }

    /**
     * Test that defragmentation can be successfuly cancelled via JMX bean.
     * @throws Exception If failed.
     */
    @Test
    public void testDefragmentationCancel() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().state(ACTIVE);

        DefragmentationMXBean mxBean = defragmentationMXBean(ignite.name());

        mxBean.schedule("");

        assertTrue(mxBean.cancel());

        // subsequent cancel call should be successful
        assertTrue(mxBean.cancel());
    }

    /**
     * Test that ongong defragmentation can be stopped via JMX bean.
     * Description:
     * 1. Start one node.
     * 2. Put a load of a data on it.
     * 3. Schedule defragmentation.
     * 4. Make IO factory slow down after 128 partitions are processed, so we have time to stop the defragmentation.
     * 5. Stop the defragmentation.
     * @throws Exception If failed.
     */
    @Test
    public void testDefragmentationCancelInProgress() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1024; i++)
            cache.put(i, i);

        forceCheckpoint(ig);

        DefragmentationMXBean mxBean = defragmentationMXBean(ig.name());

        mxBean.schedule("");

        stopGrid(0);

        blockCdl = new CountDownLatch(128);

        UnaryOperator<IgniteConfiguration> cfgOp = cfg -> {
            DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

            FileIOFactory delegate = dsCfg.getFileIOFactory();

            dsCfg.setFileIOFactory((file, modes) -> {
                if (file.getName().contains("dfrg")) {
                    if (blockCdl.getCount() == 0) {
                        try {
                            // Slow down defragmentation process.
                            // This'll be enough for the test since we have, like, 900 partitions left.
                            Thread.sleep(100);
                        }
                        catch (InterruptedException ignore) {
                            // No-op.
                        }
                    }
                    else
                        blockCdl.countDown();
                }

                return delegate.create(file, modes);
            });

            return cfg;
        };

        IgniteInternalFuture<?> fut = runAsync(() -> {
            try {
                startGrid(0, cfgOp);
            }
            catch (Exception e) {
                // No-op.
                throw new RuntimeException(e);
            }
        });

        blockCdl.await();

        mxBean = defragmentationMXBean(ig.name());

        assertTrue(mxBean.cancel());

        fut.get();

        assertTrue(mxBean.cancel());
    }

    /**
     * Test that JMX bean provides correct defragmentation status.
     * Description:
     * 1. Start one node,
     * 2. Put a load of data on it.
     * 3. Schedule defragmentation.
     * 4. Completely stop defragmentation when 128 partitions processed.
     * 5. Check defragmentation status.
     * 6. Continue defragmentation and wait for it to end.
     * 7. Check defragmentation finished.
     * @throws Exception If failed.
     */
    @Test
    public void testDefragmentationStatus() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        ig.getOrCreateCache(DEFAULT_CACHE_NAME + "1");

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME + "2");

        ig.getOrCreateCache(DEFAULT_CACHE_NAME + "3");

        for (int i = 0; i < 1024; i++)
            cache.put(i, i);

        forceCheckpoint(ig);

        DefragmentationMXBean mxBean = defragmentationMXBean(ig.name());

        mxBean.schedule("");

        stopGrid(0);

        blockCdl = new CountDownLatch(128);
        waitCdl = new CountDownLatch(1);

        UnaryOperator<IgniteConfiguration> cfgOp = cfg -> {
            DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

            FileIOFactory delegate = dsCfg.getFileIOFactory();

            dsCfg.setFileIOFactory((file, modes) -> {
                if (file.getName().contains("dfrg")) {
                    if (blockCdl.getCount() == 0) {
                        try {
                            waitCdl.await();
                        }
                        catch (InterruptedException ignore) {
                            // No-op.
                        }
                    }
                    else
                        blockCdl.countDown();
                }

                return delegate.create(file, modes);
            });

            return cfg;
        };

        IgniteInternalFuture<?> fut = runAsync(() -> {
            try {
                startGrid(0, cfgOp);
            }
            catch (Exception e) {
                // No-op.
                throw new RuntimeException(e);
            }
        });

        blockCdl.await();

        mxBean = defragmentationMXBean(ig.name());

        final IgniteKernal gridx = IgnitionEx.gridx(ig.name());
        final IgniteDefragmentation defragmentation = gridx.context().defragmentation();
        final IgniteDefragmentation.DefragmentationStatus status1 = defragmentation.status();

        assertEquals(status1.getStartTs(), mxBean.startTime());

        assertTrue(mxBean.inProgress());
        final int totalPartitions = status1.getTotalPartitions();
        assertEquals(totalPartitions, mxBean.totalPartitions());

        waitCdl.countDown();

        fut.get();

        ((GridCacheDatabaseSharedManager)grid(0).context().cache().context().database())
            .defragmentationManager()
            .completionFuture()
            .get();

        assertFalse(mxBean.inProgress());
        assertEquals(totalPartitions, mxBean.processedPartitions());
    }

//    /** */
//    private void load(long cnt, int iters, int par, int deleteChance) throws IgniteInterruptedCheckedException {
//        for (int run = 0; run < iters; ++run) {
//            AtomicLong cnt0 = new AtomicLong(0);
//
//            for (int i = 0; i < par; ++i) {
//                runAsync(() -> {
//                    Random rnd = new Random();
//
//                    while (true) {
//                        long v = cnt0.incrementAndGet();
//
//                        if (v >= cnt)
//                            break;
//
//                        Ignite ig = grid(rnd.nextInt(G.allGrids().size()));
//
//                        IgniteCache<Long, Employer> cache = ig.cache("cache" + rnd.nextInt(5));
//
//                        if (rnd.nextInt(100) >= 100 - deleteChance)
//                            cache.put(v, new Employer("emp" + v, v, randomString(rnd, 28, 1500)));
//                        else
//                            cache.remove(v);
//
//                        Thread.yield();
//                    }
//                });
//            }
//
//            waitForCondition(() -> cnt0.get() >= cnt, getTestTimeout());
//        }
//    }

    /**
     * Get defragmentation JMX bean.
     * @param name Ignite instance name.
     * @return Defragmentation JMX bean.
     */
    private DefragmentationMXBean defragmentationMXBean(String name) {
        return getMxBean(
            name,
            "Defragmentation",
            DefragmentationMXBeanImpl.class,
            DefragmentationMXBean.class
        );
    }

    /** */
    private CacheConfiguration cacheCfg(int num) {
        CacheConfiguration ccfg = new CacheConfiguration<>("cache" + num);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, CACHE_PARTS_CNT));

        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        return ccfg;
    }

//    /** */
//    public static class Employer {
//        /** */
////        @QuerySqlField
//        public String name;
//
//        /** */
////        @QuerySqlField
//        public long salary;
//
//        /** */
////        @QuerySqlField
//        public String data;
//
//        /** */
//        public Employer(String name, long salary, String data) {
//            this.name = name;
//            this.salary = salary;
//            this.data = data;
//        }
//    }
}
