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
import java.nio.MappedByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.SF;

/**
 *
 */
public class CheckpointBufferDeadlockTest extends GridCommonAbstractTest {
    /** Max size. */
    private static final int MAX_SIZE = 500 * 1024 * 1024;

    /** CP buffer size. */
    private static final int CP_BUF_SIZE = 20 * 1024 * 1024;

    /** Slow checkpoint enabled. */
    private static final AtomicBoolean slowCheckpointEnabled = new AtomicBoolean(false);

    /** Checkpoint park nanos. */
    private static final int CHECKPOINT_PARK_NANOS = 50_000_000;

    /** Entry byte chunk size. */
    private static final int ENTRY_BYTE_CHUNK_SIZE = 900;

    /** Pages touched under CP lock. */
    private static final int PAGES_TOUCHED_UNDER_CP_LOCK = 20;

    /** Slop load flag. */
    private static final AtomicBoolean stop = new AtomicBoolean(false);

    /** Checkpoint threads. */
    private int checkpointThreads;

    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, super.log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setFileIOFactory(new SlowCheckpointFileIOFactory())
                .setCheckpointThreads(checkpointThreads)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(MAX_SIZE)
                        .setCheckpointPageBufferSize(CP_BUF_SIZE)
                )
        );

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setGridLogger(log);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stop.set(false);

        slowCheckpointEnabled.set(false);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stop.set(true);

        slowCheckpointEnabled.set(false);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    public void testFourCheckpointThreads() throws Exception {
        checkpointThreads = 4;

        for (int i = 0; i < SF.applyLB(10, 3); i++) {
            beforeTest();

            try {
                runDeadlockScenario();
            }
            finally {
                afterTest();
            }
        }
    }

    /**
     *
     */
    @Test
    public void testOneCheckpointThread() throws Exception {
        checkpointThreads = 1;

        runDeadlockScenario();
    }

    /**
     *
     */
    private void runDeadlockScenario() throws Exception {
        LogListener lsnr = LogListener.matches(s -> s.contains("AssertionError")).build();

        log.registerListener(lsnr);

        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig.context().cache().context().database();

        FilePageStoreManager pageStoreMgr = (FilePageStoreManager)ig.context().cache().context().pageStore();

        final String cacheName = "single-part";

        CacheConfiguration<Object, Object> cacheCfg = new CacheConfiguration<>()
                .setName(cacheName)
                .setAffinity(new RendezvousAffinityFunction(false, 1));

        IgniteCache<Object, Object> singlePartCache = ig.getOrCreateCache(cacheCfg);

        db.enableCheckpoints(false).get();

        Thread.sleep(1_000);

        try (IgniteDataStreamer<Object, Object> streamer = ig.dataStreamer(singlePartCache.getName())) {
            int entries = MAX_SIZE / ENTRY_BYTE_CHUNK_SIZE / 4;

            for (int i = 0; i < entries; i++)
                streamer.addData(i, new byte[ENTRY_BYTE_CHUNK_SIZE]);

            streamer.flush();
        }

        slowCheckpointEnabled.set(true);
        log.info(">>> Slow checkpoints enabled");

        db.enableCheckpoints(true).get();

        AtomicBoolean fail = new AtomicBoolean(false);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int loops = 0;

                while (!stop.get()) {
                    if (loops % 10 == 0 && loops > 0 && loops < 500 || loops % 500 == 0 && loops >= 500)
                        log.info("Successfully completed " + loops + " loops");

                    db.checkpointReadLock();

                    try {
                        Set<FullPageId> pickedPagesSet = new HashSet<>();

                        PageStore store = pageStoreMgr.getStore(CU.cacheId(cacheName), 0);

                        int pages = store.pages();

                        DataRegion region = db.dataRegion(DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME);

                        PageMemoryImpl pageMem = (PageMemoryImpl)region.pageMemory();

                        while (pickedPagesSet.size() < PAGES_TOUCHED_UNDER_CP_LOCK) {
                            int pageIdx = ThreadLocalRandom.current().nextInt(
                                PAGES_TOUCHED_UNDER_CP_LOCK, pages - PAGES_TOUCHED_UNDER_CP_LOCK);

                            long pageId = PageIdUtils.pageId(0, PageIdAllocator.FLAG_DATA, pageIdx);

                            pickedPagesSet.add(new FullPageId(pageId, CU.cacheId(cacheName)));
                        }

                        List<FullPageId> pickedPages = new ArrayList<>(pickedPagesSet);

                        assertEquals(PAGES_TOUCHED_UNDER_CP_LOCK, pickedPages.size());

                        // Sort to avoid deadlocks on pages rw-locks.
                        pickedPages.sort(new Comparator<FullPageId>() {
                            @Override public int compare(FullPageId o1, FullPageId o2) {
                                int cmp = Long.compare(o1.groupId(), o2.groupId());

                                if (cmp != 0)
                                    return cmp;

                                return Long.compare(o1.effectivePageId(), o2.effectivePageId());
                            }
                        });

                        List<Long> readLockedPages = new ArrayList<>();

                        // Read lock many pages at once intentionally.
                        for (int i = 0; i < PAGES_TOUCHED_UNDER_CP_LOCK / 2; i++) {
                            FullPageId fpid = pickedPages.get(i);

                            long page = pageMem.acquirePage(fpid.groupId(), fpid.pageId());

                            long abs = pageMem.readLock(fpid.groupId(), fpid.pageId(), page);

                            assertFalse(fpid.toString(), abs == 0);

                            readLockedPages.add(page);
                        }

                        // Emulate writes to trigger throttling.
                        for (int i = PAGES_TOUCHED_UNDER_CP_LOCK / 2; i < PAGES_TOUCHED_UNDER_CP_LOCK && !stop.get(); i++) {
                            FullPageId fpid = pickedPages.get(i);

                            long page = pageMem.acquirePage(fpid.groupId(), fpid.pageId());

                            long abs = pageMem.writeLock(fpid.groupId(), fpid.pageId(), page);

                            assertFalse(fpid.toString(), abs == 0);

                            pageMem.writeUnlock(fpid.groupId(), fpid.pageId(), page, null, true);

                            pageMem.releasePage(fpid.groupId(), fpid.pageId(), page);
                        }

                        for (int i = 0; i < PAGES_TOUCHED_UNDER_CP_LOCK / 2; i++) {
                            FullPageId fpid = pickedPages.get(i);

                            pageMem.readUnlock(fpid.groupId(), fpid.pageId(), readLockedPages.get(i));

                            pageMem.releasePage(fpid.groupId(), fpid.pageId(), readLockedPages.get(i));
                        }
                    }
                    catch (Throwable e) {
                        log.error("Error in loader thread", e);

                        fail.set(true);
                    }
                    finally {
                        db.checkpointReadUnlock();
                    }

                    loops++;
                }

            }
        }, 10, "load-runner");

        Thread.sleep(10_000); // Await for the start of throttling.

        slowCheckpointEnabled.set(false);
        log.info(">>> Slow checkpoints disabled");

        assertFalse(fail.get());

        forceCheckpoint(); // Previous checkpoint should eventually finish.

        stop.set(true);

        fut.get();

        db.enableCheckpoints(true).get();

        //check that there is no problem with pinned pages
        ig.destroyCache(cacheName);

        assertFalse(lsnr.check());

        log.unregisterListener(lsnr);
    }

    /**
     * Create File I/O that emulates poor checkpoint write speed.
     */
    private static class SlowCheckpointFileIOFactory implements FileIOFactory {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Delegate factory. */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... openOption) throws IOException {
            final FileIO delegate = delegateFactory.create(file, openOption);

            return new FileIODecorator(delegate) {
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    parkIfNeeded();

                    return delegate.write(srcBuf);
                }

                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    parkIfNeeded();

                    return delegate.write(srcBuf, position);
                }

                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    parkIfNeeded();

                    return delegate.write(buf, off, len);
                }

                /**
                 * Parks current checkpoint thread if slow mode is enabled.
                 */
                private void parkIfNeeded() {
                    if (slowCheckpointEnabled.get() && Thread.currentThread().getName().contains("checkpoint"))
                        LockSupport.parkNanos(CHECKPOINT_PARK_NANOS);
                }

                /** {@inheritDoc} */
                @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
                    return delegate.map(sizeBytes);
                }
            };
        }
    }

}
