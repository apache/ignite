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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class LocalWalModeChangeDuringRebalancingSelfTest extends GridCommonAbstractTest {
    /** */
    private static boolean disableWalDuringRebalancing = true;

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
                        .setMaxSize(200 * 1024 * 1024)
                        .setInitialSize(200 * 1024 * 1024)
                )
                // Test verifies checkpoint count, so it is essencial that no checkpoint is triggered by timeout
                .setCheckpointFrequency(999_999_999_999L)
                .setFileIOFactory(new TestFileIOFactory(new DataStorageConfiguration().getFileIOFactory()))
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                // Test checks internal state before and after rebalance, so it is configured to be triggered manually
                .setRebalanceDelay(-1),

            new CacheConfiguration(REPL_CACHE)
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
    public void testWalDisabledDuringRebalancing() throws Exception {
        doTestSimple();
    }

    /**
     * @throws Exception If failed.
     */
    public void testWalNotDisabledIfParameterSetToFalse() throws Exception {
        disableWalDuringRebalancing = false;

        doTestSimple();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestSimple() throws Exception {
        Ignite ignite = startGrids(3);

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
    public void testLocalAndGlobalWalStateInterdependence() throws Exception {
        Ignite ignite = startGrids(3);

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
    public void testWithExchangesMerge() throws Exception {
        final int nodeCnt = 5;
        final int keyCnt = getKeysCount();

        Ignite ignite = startGrids(nodeCnt);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(REPL_CACHE);

        for (int k = 0; k < keyCnt; k++)
            cache.put(k, k);

        stopGrid(2);
        stopGrid(3);
        stopGrid(4);

        // Rewrite data to trigger further rebalance.
        for (int k = 0; k < keyCnt; k++)
            cache.put(k, k * 2);

        // Start several grids in parallel to trigger exchanges merge.
        startGridsMultiThreaded(2, 3);

        for (int nodeIdx = 2; nodeIdx < nodeCnt; nodeIdx++) {
            CacheGroupContext grpCtx = grid(nodeIdx).cachex(REPL_CACHE).context().group();

            assertFalse(grpCtx.walEnabled());
        }

        // Invoke rebalance manually.
        for (Ignite g : G.allGrids())
            g.cache(REPL_CACHE).rebalance();

        awaitPartitionMapExchange();

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
    public void testParallelExchangeDuringRebalance() throws Exception {
        doTestParallelExchange(supplyMessageLatch);
    }

    /**
     * @throws Exception If failed.
     */
    public void testParallelExchangeDuringCheckpoint() throws Exception {
        doTestParallelExchange(fileIOLatch);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestParallelExchange(AtomicReference<CountDownLatch> latchRef) throws Exception {
        Ignite ignite = startGrids(3);

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

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

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
    public void testDataClearedAfterRestartWithDisabledWal() throws Exception {
        Ignite ignite = startGrid(0);

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
            assertFalse("k=" + k +", v=" + cache.get(k), cache.containsKey(k));
    }

    /**
     * @throws Exception If failed.
     */
    public void testWalNotDisabledAfterShrinkingBaselineTopology() throws Exception {
        Ignite ignite = startGrids(4);

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
        @Override public FileIO create(File file) throws IOException {
            return new TestFileIO(delegate.create(file));
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
}
