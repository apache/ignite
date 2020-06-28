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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class IgniteSequentialNodeCrashRecoveryTest extends GridCommonAbstractTest {
    /** */
    private static final int PAGE_SIZE = 4096;

    /** */
    private FileIOFactory fileIoFactory;

    /** */
    private FailureHandler failureHnd;

    /** */
    private DiscoverySpi discoverySpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(256 * 1024 * 1024).setPersistenceEnabled(true))
            // Set large checkpoint frequency to make sure no checkpoint happens right after the node start.
            .setCheckpointFrequency(getTestTimeout())
            .setPageSize(PAGE_SIZE);

        if (fileIoFactory != null)
            dsCfg.setFileIOFactory(fileIoFactory);

        if (discoverySpi != null)
            cfg.setDiscoverySpi(discoverySpi);

        cfg
            .setDataStorageConfiguration(dsCfg)
            .setConsistentId(igniteInstanceName);

        if (failureHnd != null)
            cfg.setFailureHandler(failureHnd);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCrashOnCheckpointAfterLogicalRecovery() throws Exception {
        IgniteEx g = startGrid(0);

        g.cluster().active(true);

        g.getOrCreateCache(new CacheConfiguration<>("cache")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 8)));

        disableCheckpoints(g);

        {
            IgniteCache<Object, Object> cache = g.cache("cache");

            // Now that checkpoints are disabled, put some data to the cache.
            GridTestUtils.runMultiThreaded(() -> {
                for (int i = 0; i < 400; i++)
                    cache.put(i % 100, Thread.currentThread().getName());
            }, 64, "update-thread");
        }

        Collection<FullPageId> dirtyAfterLoad = captureDirtyPages(g);

        stopGrid(0);

        CheckpointFailingIoFactory f = (CheckpointFailingIoFactory)
            (fileIoFactory = new CheckpointFailingIoFactory(false));

        StopLatchFailureHandler fh = (StopLatchFailureHandler)(failureHnd = new StopLatchFailureHandler());

        //Blocking first exchange to prevent checkpoint on node start(reason = 'node started').
        BlockingDiscoverySpi ds = (BlockingDiscoverySpi)
            (discoverySpi = new BlockingDiscoverySpi((m) -> m instanceof ChangeGlobalStateMessage));

        // Now start the node. Since the checkpoint was disabled, logical recovery will be performed.
        g = startGrid(0);

        fileIoFactory = null;
        failureHnd = null;

        // Capture dirty pages after logical recovery & updates.
        Collection<FullPageId> dirtyAfterRecoveryAndUpdates = captureDirtyPages(g);

        f.startFailing();

        ds.clearBlock();

        triggerCheckpoint(g);

        assertTrue("Failed to wait for checkpoint failure", fh.waitFailed());

        // Capture pages we marked on first run and did not mark on second run.
        dirtyAfterLoad.removeAll(dirtyAfterRecoveryAndUpdates);

        assertFalse(dirtyAfterLoad.isEmpty());

        fileIoFactory = new CheckingIoFactory(dirtyAfterLoad);
        discoverySpi = null;

        g = startGrid(0);

        {
            IgniteCache<Object, Object> cache = g.cache("cache");

            for (int i = 0; i < 400; i++)
                cache.put(100 + (i % 100), Thread.currentThread().getName());

            for (int i = 0; i < 200; i++)
                assertTrue("i=" + i, cache.containsKey(i));
        }
    }

    /**
     *
     */
    private void disableCheckpoints(IgniteEx g) throws Exception {
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)g.context()
            .cache().context().database();

        dbMgr.enableCheckpoints(false).get();
    }

    /**
     * @param ig Ignite instance.
     */
    private void triggerCheckpoint(IgniteEx ig) {
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ig.context()
            .cache().context().database();

        dbMgr.wakeupForCheckpoint("test-should-fail");
    }

    /**
     * @param g Ignite instance.
     * @throws IgniteCheckedException If failed.
     */
    private Collection<FullPageId> captureDirtyPages(IgniteEx g) throws IgniteCheckedException {
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)g.context()
            .cache().context().database();

        dbMgr.checkpointReadLock();
        try {
            //Moving free list pages to offheap.
            for (CacheGroupContext group : g.context().cache().cacheGroups()) {
                ((GridCacheOffheapManager)group.offheap()).onMarkCheckpointBegin(new DummyCheckpointContext());
            }
        }
        finally {
            dbMgr.checkpointReadUnlock();
        }

        // Capture a set of dirty pages.
        PageMemoryImpl pageMem = (PageMemoryImpl)dbMgr.dataRegion("default").pageMemory();

        return pageMem.dirtyPages();
    }

    /**
     *
     */
    private class StopLatchFailureHandler extends StopNodeFailureHandler {
        /** */
        private CountDownLatch stopLatch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public boolean handle(Ignite ignite, FailureContext failureCtx) {
            new Thread(
                new Runnable() {
                    @Override public void run() {
                        U.error(ignite.log(), "Stopping local node on Ignite failure: [failureCtx=" + failureCtx + ']');

                        IgnitionEx.stop(ignite.name(), true, true);

                        stopLatch.countDown();
                    }
                },
                "node-stopper"
            ).start();

            return true;
        }

        /**
         * @return {@code true} if wait succeeded.
         * @throws InterruptedException If current thread was interrupted.
         */
        public boolean waitFailed() throws InterruptedException {
            return stopLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StopNodeFailureHandler.class, this, "super", super.toString());
        }
    }

    /** */
    private static class DummyCheckpointContext implements DbCheckpointListener.Context {
        /** {@inheritDoc} */
        @Override public boolean nextSnapshot() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<?> finishedStateFut() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public PartitionAllocationMap partitionStatMap() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean needToSnapshot(String cacheOrGrpName) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Executor executor() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean hasPages() {
            return false;
        }
    }

    /** */
    protected static class BlockingDiscoverySpi extends TcpDiscoverySpi {
        /** Discovery custom message filter. */
        private volatile IgnitePredicate<DiscoveryCustomMessage> blockPred;

        /** **/
        public BlockingDiscoverySpi(IgnitePredicate<DiscoveryCustomMessage> blockPred) {
            this.blockPred = blockPred;
        }

        /** {@inheritDoc} */
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryCustomEventMessage) {
                IgnitePredicate<DiscoveryCustomMessage> pred = blockPred;

                if (pred != null && pred.apply(extractCustomMessage((TcpDiscoveryCustomEventMessage)msg))) {
                    try {
                        GridTestUtils.waitForCondition(() -> blockPred == null, 20_000);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        log.error("Fail to await release", e);
                    }
                }
            }
        }

        /** */
        private DiscoveryCustomMessage extractCustomMessage(TcpDiscoveryCustomEventMessage msg) {
            DiscoverySpiCustomMessage msgObj = null;

            try {
                msgObj = msg.message(marshaller(), U.resolveClassLoader(ignite().configuration()));
            }
            catch (Throwable e) {
                U.error(log, "Failed to unmarshal discovery custom message.", e);
            }

            return ((CustomMessageWrapper)msgObj).delegate();
        }

        /** Unblock discovery custom messages. */
        public void clearBlock() {
            blockPred = null;
        }
    }

    /**
     *
     */
    private static class CheckingIoFactory implements FileIOFactory {
        /** */
        private final transient Collection<FullPageId> forbiddenPages;

        /**
         * @param forbiddenPages Forbidden pages.
         */
        private CheckingIoFactory(Collection<FullPageId> forbiddenPages) {
            this.forbiddenPages = forbiddenPages;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = new RandomAccessFileIOFactory().create(file, modes);

            if (file.getName().contains("part-"))
                return new CheckingFileIO(file, delegate, forbiddenPages);

            return delegate;
        }
    }

    /**
     *
     */
    private static class CheckingFileIO extends FileIODecorator {
        /** */
        private int grpId;

        /** */
        private int partId;

        /** */
        private Collection<FullPageId> forbiddenPages;

        /**
         * @param file File.
         * @param delegate Delegate.
         * @param forbiddenPages Forbidden pages.
         */
        public CheckingFileIO(File file, FileIO delegate, Collection<FullPageId> forbiddenPages) {
            super(delegate);
            this.forbiddenPages = forbiddenPages;

            String fileName = file.getName();

            int start = fileName.indexOf("part-") + 5;
            int end = fileName.indexOf(".bin");
            partId = Integer.parseInt(fileName.substring(start, end));

            String path = file.getPath();

            if (path.contains(File.separator + "metastorage" + File.separator))
                grpId = MetaStorage.METASTORAGE_CACHE_ID;
            else {
                start = path.indexOf("cache-") + 6;
                end = path.indexOf(File.separator, start);

                grpId = start >= 0 ? CU.cacheId(path.substring(start, end)) : 0;
            }
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf) throws IOException {
            throw new AssertionError("Should not be called");
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
            FullPageId fId = new FullPageId(
                PageIdUtils.pageId(partId, PageIdAllocator.FLAG_DATA, (int)(position / PAGE_SIZE) - 1),
                grpId);

            if (forbiddenPages.contains(fId))
                throw new AssertionError("Attempted to write invalid page on recovery: " + fId);

            return super.write(srcBuf, position);
        }

        /** {@inheritDoc} */
        @Override public int write(byte[] buf, int off, int len) throws IOException {
            throw new AssertionError("Should not be called");
        }
    }
}
