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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.OpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointHistory;
import org.apache.ignite.internal.processors.cache.persistence.file.AbstractFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class LocalWalModeChangeDuringRebalancingSelfTest extends GridCommonAbstractTest {
    /** */
    private static boolean disableWalDuringRebalancing = true;

    /** */
    private static boolean enablePendingTxTracker = false;

    /** */
    private static int dfltCacheBackupCnt = 0;

    /** */
    private static final AtomicReference<CountDownLatch> supplyMessageLatch = new AtomicReference<>();

    /** */
    private static final AtomicReference<CountDownLatch> fileIOLatch = new AtomicReference<>();

    /** Replicated cache name. */
    private static final String REPL_CACHE = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                )
                // Test verifies checkpoint count, so it is essential that no checkpoint is triggered by timeout
                .setCheckpointFrequency(999_999_999_999L)
                .setWalMode(WALMode.LOG_ONLY)
                .setFileIOFactory(new TestFileIOFactory(new DataStorageConfiguration().getFileIOFactory()))
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                // Test checks internal state before and after rebalance, so it is configured to be triggered manually
                .setRebalanceDelay(-1)
                .setBackups(dfltCacheBackupCnt),

            new CacheConfiguration(REPL_CACHE)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setRebalanceDelay(-1)
                .setCacheMode(CacheMode.REPLICATED)
        );

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                    int grpId = ((GridDhtPartitionSupplyMessage)((GridIoMessage)msg).message()).groupId();

                    if (grpId == CU.cacheId(DEFAULT_CACHE_NAME)) {
                        CountDownLatch latch0 = supplyMessageLatch.get();

                        if (latch0 != null)
                            try {
                                latch0.await();
                            }
                            catch (InterruptedException ex) {
                                throw new IgniteException(ex);
                            }
                    }
                }

                super.sendMessage(node, msg);
            }

            @Override public void sendMessage(ClusterNode node, Message msg,
                IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                    int grpId = ((GridDhtPartitionSupplyMessage)((GridIoMessage)msg).message()).groupId();

                    if (grpId == CU.cacheId(DEFAULT_CACHE_NAME)) {
                        CountDownLatch latch0 = supplyMessageLatch.get();

                        if (latch0 != null)
                            try {
                                latch0.await();
                            }
                            catch (InterruptedException ex) {
                                throw new IgniteException(ex);
                            }
                    }
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        cfg.setConsistentId(igniteInstanceName);

        System.setProperty(IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING,
            Boolean.toString(disableWalDuringRebalancing));

        System.setProperty(IgniteSystemProperties.IGNITE_PENDING_TX_TRACKER_ENABLED,
            Boolean.toString(enablePendingTxTracker));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        CountDownLatch msgLatch = supplyMessageLatch.get();

        if (msgLatch != null) {
            while (msgLatch.getCount() > 0)
                msgLatch.countDown();

            supplyMessageLatch.set(null);
        }

        CountDownLatch fileLatch = fileIOLatch.get();

        if (fileLatch != null) {
            while (fileLatch.getCount() > 0)
                fileLatch.countDown();

            fileIOLatch.set(null);
        }

        stopAllGrids();

        cleanPersistenceDir();

        disableWalDuringRebalancing = true;
        enablePendingTxTracker = false;
        dfltCacheBackupCnt = 0;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING);

        System.clearProperty(IgniteSystemProperties.IGNITE_PENDING_TX_TRACKER_ENABLED);
    }

    /**
     * @return Count of entries to be processed within test.
     */
    protected int getKeysCount() {
        return 10_000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWalDisabledDuringRebalancing() throws Exception {
        doTestSimple();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWalNotDisabledIfParameterSetToFalse() throws Exception {
        disableWalDuringRebalancing = false;

        doTestSimple();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestSimple() throws Exception {
        Ignite ignite = startGrids(3);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int keysCnt = getKeysCount();

        for (int k = 0; k < keysCnt; k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(3);

        final CheckpointHistory cpHist =
            ((GridCacheDatabaseSharedManager)newIgnite.context().cache().context().database()).checkpointHistory();

        waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !cpHist.checkpoints().isEmpty();
            }
        }, 10_000);

        U.sleep(10); // To ensure timestamp granularity.

        long newIgniteStartedTimestamp = System.currentTimeMillis();

        newIgnite.cluster().setBaselineTopology(4);

        awaitExchange(newIgnite);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        assertEquals(!disableWalDuringRebalancing, grpCtx.walEnabled());

        U.sleep(10); // To ensure timestamp granularity.

        long rebalanceStartedTimestamp = System.currentTimeMillis();

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertTrue(grpCtx.walEnabled());

        U.sleep(10); // To ensure timestamp granularity.

        long rebalanceFinishedTimestamp = System.currentTimeMillis();

        for (Integer k = 0; k < keysCnt; k++)
            assertEquals("k=" + k, k, cache.get(k));

        int checkpointsBeforeNodeStarted = 0;
        int checkpointsBeforeRebalance = 0;
        int checkpointsAfterRebalance = 0;

        for (Long timestamp : cpHist.checkpoints()) {
            if (timestamp < newIgniteStartedTimestamp)
                checkpointsBeforeNodeStarted++;
            else if (timestamp >= newIgniteStartedTimestamp && timestamp < rebalanceStartedTimestamp)
                checkpointsBeforeRebalance++;
            else if (timestamp >= rebalanceStartedTimestamp && timestamp <= rebalanceFinishedTimestamp)
                checkpointsAfterRebalance++;
        }

        assertEquals(1, checkpointsBeforeNodeStarted); // checkpoint on start
        assertEquals(0, checkpointsBeforeRebalance);
        assertEquals(disableWalDuringRebalancing ? 1 : 0, checkpointsAfterRebalance); // checkpoint if WAL was re-activated
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWalDisabledDuringRebalancingWithPendingTxTracker() throws Exception {
        enablePendingTxTracker = true;
        dfltCacheBackupCnt = 2;

        Ignite ignite = startGrids(3);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().active(true);

        ignite.cluster().setBaselineTopology(3);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        stopGrid(2);

        awaitExchange((IgniteEx)ignite);

        // Ensure each partition has received an update.
        for (int k = 0; k < RendezvousAffinityFunction.DFLT_PARTITION_COUNT; k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(2);

        awaitExchange(newIgnite);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        assertFalse(grpCtx.walEnabled());

        long rebalanceStartedTs = System.currentTimeMillis();

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertTrue(grpCtx.walEnabled());

        long rebalanceFinishedTs = System.currentTimeMillis();

        CheckpointHistory cpHist =
            ((GridCacheDatabaseSharedManager)newIgnite.context().cache().context().database()).checkpointHistory();

        assertNotNull(cpHist);

        // Ensure there was a checkpoint on WAL re-activation.
        assertEquals(
            1,
            cpHist.checkpoints()
                .stream()
                .filter(ts -> rebalanceStartedTs <= ts && ts <= rebalanceFinishedTs)
                .count());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalAndGlobalWalStateInterdependence() throws Exception {
        Ignite ignite = startGrids(3);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < getKeysCount(); k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(3);

        newIgnite.cluster().setBaselineTopology(ignite.cluster().nodes());

        awaitExchange(newIgnite);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        assertFalse(grpCtx.walEnabled());

        ignite.cluster().disableWal(DEFAULT_CACHE_NAME);

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertFalse(grpCtx.walEnabled()); // WAL is globally disabled

        ignite.cluster().enableWal(DEFAULT_CACHE_NAME);

        assertTrue(grpCtx.walEnabled());
    }

    /**
     * Test that local WAL mode changing works well with exchanges merge.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWithExchangesMerge() throws Exception {
        final int nodeCnt = 4;
        final int keyCnt = getKeysCount();

        Ignite ignite = startGrids(nodeCnt);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(REPL_CACHE);

        for (int k = 0; k < keyCnt; k++)
            cache.put(k, k);

        stopGrid(2);
        stopGrid(3);

        // Rewrite data to trigger further rebalance.
        for (int k = 0; k < keyCnt; k++)
            cache.put(k, k * 2);

        // Start several grids in parallel to trigger exchanges merge.
        startGridsMultiThreaded(2, 2);

        for (int nodeIdx = 2; nodeIdx < nodeCnt; nodeIdx++) {
            CacheGroupContext grpCtx = grid(nodeIdx).cachex(REPL_CACHE).context().group();

            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return !grpCtx.walEnabled();
                }
            }, 5_000));
        }

        // Invoke rebalance manually.
        for (Ignite g : G.allGrids())
            g.cache(REPL_CACHE).rebalance();

        awaitPartitionMapExchange(false, false, null, false, Collections.singleton(REPL_CACHE));

        for (int nodeIdx = 2; nodeIdx < nodeCnt; nodeIdx++) {
            CacheGroupContext grpCtx = grid(nodeIdx).cachex(REPL_CACHE).context().group();

            assertTrue(grpCtx.walEnabled());
        }

        // Check no data loss.
        for (int nodeIdx = 2; nodeIdx < nodeCnt; nodeIdx++) {
            IgniteCache<Integer, Integer> cache0 = grid(nodeIdx).cache(REPL_CACHE);

            for (int k = 0; k < keyCnt; k++)
                Assert.assertEquals("nodeIdx=" + nodeIdx + ", key=" + k, (Integer)(2 * k), cache0.get(k));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testParallelExchangeDuringRebalance() throws Exception {
        doTestParallelExchange(supplyMessageLatch);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testParallelExchangeDuringCheckpoint() throws Exception {
        doTestParallelExchange(fileIOLatch);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestParallelExchange(AtomicReference<CountDownLatch> latchRef) throws Exception {
        Ignite ignite = startGrids(3);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < getKeysCount(); k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(3);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        CountDownLatch latch = new CountDownLatch(1);

        latchRef.set(latch);

        ignite.cluster().setBaselineTopology(ignite.cluster().nodes());

        // Await fully exchange complete.
        awaitExchange(newIgnite);

        assertFalse(grpCtx.walEnabled());

        // TODO : test with client node as well
        startGrid(4); // Trigger exchange

        assertFalse(grpCtx.walEnabled());

        latch.countDown();

        assertFalse(grpCtx.walEnabled());

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertTrue(waitForCondition(grpCtx::walEnabled, 2_000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataClearedAfterRestartWithDisabledWal() throws Exception {
        Ignite ignite = startGrid(0);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int keysCnt = getKeysCount();

        for (int k = 0; k < keysCnt; k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(1);

        newIgnite.cluster().setBaselineTopology(2);

        // Await fully exchange complete.
        awaitExchange(newIgnite);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        assertFalse(grpCtx.localWalEnabled());

        stopGrid(1);
        stopGrid(0);

        newIgnite = startGrid(1);

        newIgnite.cluster().active(true);

        newIgnite.cluster().setBaselineTopology(newIgnite.cluster().nodes());

        cache = newIgnite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < keysCnt; k++)
            assertFalse("k=" + k + ", v=" + cache.get(k), cache.containsKey(k));

        Collection<Integer> lostParts = cache.lostPartitions();

        Set<Integer> keys = new TreeSet<>();

        for (int k = 0; k < keysCnt; k++) {
            // Skip lost partitions.
            if (lostParts.contains(newIgnite.affinity(DEFAULT_CACHE_NAME).partition(k)))
                continue;

            keys.add(k);
        }

        for (Integer k : keys)
            assertFalse("k=" + k + ", v=" + cache.get(k), cache.containsKey(k));

        assertFalse(cache.containsKeys(keys));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWalNotDisabledAfterShrinkingBaselineTopology() throws Exception {
        Ignite ignite = startGrids(4);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int keysCnt = getKeysCount();

        for (int k = 0; k < keysCnt; k++)
            cache.put(k, k);

        for (Ignite g : G.allGrids()) {
            CacheGroupContext grpCtx = ((IgniteEx)g).cachex(DEFAULT_CACHE_NAME).context().group();

            assertTrue(grpCtx.walEnabled());
        }

        stopGrid(2);

        ignite.cluster().setBaselineTopology(5);

        ignite.resetLostPartitions(Collections.singleton(DEFAULT_CACHE_NAME));

        // Await fully exchange complete.
        awaitExchange((IgniteEx)ignite);

        for (Ignite g : G.allGrids()) {
            CacheGroupContext grpCtx = ((IgniteEx)g).cachex(DEFAULT_CACHE_NAME).context().group();

            assertTrue(grpCtx.walEnabled());

            g.cache(DEFAULT_CACHE_NAME).rebalance();
        }

        awaitPartitionMapExchange();

        for (Ignite g : G.allGrids()) {
            CacheGroupContext grpCtx = ((IgniteEx)g).cachex(DEFAULT_CACHE_NAME).context().group();

            assertTrue(grpCtx.walEnabled());
        }
    }

    /**
     *
     * @param ig Ignite.
     */
    private void awaitExchange(IgniteEx ig) throws IgniteCheckedException {
        ig.context().cache().context().exchange().lastTopologyFuture().get();
    }

    /**
     * Put random values to cache in multiple threads until time interval given expires.
     *
     * @param cache Cache to modify.
     * @param threadCnt Number ot threads to be used.
     * @param duration Time interval in milliseconds.
     * @throws Exception When something goes wrong.
     */
    private void doLoad(IgniteCache<Integer, Integer> cache, int threadCnt, long duration) throws Exception {
        GridTestUtils.runMultiThreaded(() -> {
            long stopTs = U.currentTimeMillis() + duration;

            int keysCnt = getKeysCount();

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            do {
                try {
                    cache.put(rnd.nextInt(keysCnt), rnd.nextInt());
                }
                catch (Exception ex) {
                    MvccFeatureChecker.assertMvccWriteConflict(ex);
                }
            }
            while (U.currentTimeMillis() < stopTs);
        }, threadCnt, "load-cache");
    }

    /**
     *
     */
    private static class TestFileIOFactory implements FileIOFactory {
        /** */
        private final FileIOFactory delegate;

        /**
         * @param delegate Delegate.
         */
        TestFileIOFactory(FileIOFactory delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new TestFileIO(delegate.create(file, modes));
        }
    }

    /**
     *
     */
    private static class TestFileIO extends AbstractFileIO {
        /** */
        private final FileIO delegate;

        /**
         * @param delegate Delegate.
         */
        TestFileIO(FileIO delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public int getFileSystemBlockSize() {
            return delegate.getFileSystemBlockSize();
        }

        /** {@inheritDoc} */
        @Override public long getSparseSize() {
            return delegate.getSparseSize();
        }

        /** {@inheritDoc} */
        @Override public int punchHole(long position, int len) {
            return delegate.punchHole(position, len);
        }

        /** {@inheritDoc} */
        @Override public long position() throws IOException {
            return delegate.position();
        }

        /** {@inheritDoc} */
        @Override public void position(long newPosition) throws IOException {
            delegate.position(newPosition);
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf) throws IOException {
            return delegate.read(destBuf);
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf, long position) throws IOException {
            return delegate.read(destBuf, position);
        }

        /** {@inheritDoc} */
        @Override public int read(byte[] buf, int off, int len) throws IOException {
            return delegate.read(buf, off, len);
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf) throws IOException {
            CountDownLatch latch = fileIOLatch.get();

            if (latch != null && Thread.currentThread().getName().contains("checkpoint"))
                try {
                    latch.await();
                }
                catch (InterruptedException ex) {
                    throw new IgniteException(ex);
                }

            return delegate.write(srcBuf);
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
            CountDownLatch latch = fileIOLatch.get();

            if (latch != null && Thread.currentThread().getName().contains("checkpoint"))
                try {
                    latch.await();
                }
                catch (InterruptedException ex) {
                    throw new IgniteException(ex);
                }

            return delegate.write(srcBuf, position);
        }

        /** {@inheritDoc} */
        @Override public int write(byte[] buf, int off, int len) throws IOException {
            CountDownLatch latch = fileIOLatch.get();

            if (latch != null && Thread.currentThread().getName().contains("checkpoint"))
                try {
                    latch.await();
                }
                catch (InterruptedException ex) {
                    throw new IgniteException(ex);
                }

            return delegate.write(buf, off, len);
        }

        /** {@inheritDoc} */
        @Override public MappedByteBuffer map(int maxWalSegmentSize) throws IOException {
            return delegate.map(maxWalSegmentSize);
        }

        /** {@inheritDoc} */
        @Override public void force() throws IOException {
            delegate.force();
        }

        /** {@inheritDoc} */
        @Override public void force(boolean withMetadata) throws IOException {
            delegate.force(withMetadata);
        }

        /** {@inheritDoc} */
        @Override public long size() throws IOException {
            return delegate.size();
        }

        /** {@inheritDoc} */
        @Override public void clear() throws IOException {
            delegate.clear();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            delegate.close();
        }
    }

    /** {@inheritDoc} */
    @Override protected long getPartitionMapExchangeTimeout() {
        return super.getPartitionMapExchangeTimeout() * 2;
    }
}
