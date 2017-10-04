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

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.TrackingPageIO;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Test simulated chekpoints,
 * Disables integrated check pointer thread
 */
public class IgnitePdsCheckpointSimulationWithRealCpDisabledTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int TOTAL_PAGES = 1000;

    /** */
    private static final boolean VERBOSE = false;

    /** Cache name. */
    private static final String cacheName = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(cacheName);

        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);

        cfg.setCacheConfiguration(ccfg);

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        cfg.setMemoryConfiguration(dbCfg);

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration()
                .setCheckpointingFrequency(500)
                .setWalMode(WALMode.LOG_ONLY)
                .setAlwaysWriteFullPages(true)
        );

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /**
     * @throws Exception if failed.
     */
    public void testCheckpointSimulationMultiThreaded() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.active(true);

        GridCacheSharedContext<Object, Object> shared = ig.context().cache().context();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)shared.database();

        IgnitePageStoreManager pageStore = shared.pageStore();

        U.sleep(1_000);

        // Disable integrated checkpoint thread.
        dbMgr.enableCheckpoints(false).get();

        // Must put something in partition 0 in order to initialize meta page.
        // Otherwise we will violate page store integrity rules.
        ig.cache(cacheName).put(0, 0);

        PageMemory mem = shared.database().memoryPolicy(null).pageMemory();

        IgniteBiTuple<Map<FullPageId, Integer>, WALPointer> res;

        try {
            res = runCheckpointing(ig, (PageMemoryImpl)mem, pageStore, shared.wal(),
                shared.cache().cache(cacheName).context().cacheId());
        }
        catch (Throwable th) {
            log().error("Error while running checkpointing", th);

            throw th;
        }
        finally {
            dbMgr.enableCheckpoints(true).get();

            stopAllGrids(false);
        }

        ig = startGrid(0);

        ig.active(true);

        shared = ig.context().cache().context();

        dbMgr = (GridCacheDatabaseSharedManager)shared.database();

        dbMgr.enableCheckpoints(false).get();

        mem = shared.database().memoryPolicy(null).pageMemory();

        verifyReads(res.get1(), mem, res.get2(), shared.wal());
    }

    /**
     * @throws Exception if failed.
     */
    public void testGetForInitialWrite() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.active(true);

        GridCacheSharedContext<Object, Object> shared = ig.context().cache().context();

        int cacheId = shared.cache().cache(cacheName).context().cacheId();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)shared.database();

        // Disable integrated checkpoint thread.
        dbMgr.enableCheckpoints(false);

        PageMemory mem = shared.database().memoryPolicy(null).pageMemory();

        IgniteWriteAheadLogManager wal = shared.wal();

        WALPointer start = wal.log(new CheckpointRecord(null));

        final FullPageId[] initWrites = new FullPageId[10];

        ig.context().cache().context().database().checkpointReadLock();

        try {
            for (int i = 0; i < initWrites.length; i++)
                initWrites[i] = new FullPageId(mem.allocatePage(cacheId, 0, PageIdAllocator.FLAG_DATA), cacheId);

            // Check getForInitialWrite methods.
            for (FullPageId fullId : initWrites) {
                long page = mem.acquirePage(fullId.groupId(), fullId.pageId());
                try {
                    long pageAddr = mem.writeLock(fullId.groupId(), fullId.pageId(), page);

                    try {
                        DataPageIO.VERSIONS.latest().initNewPage(pageAddr, fullId.pageId(), mem.pageSize());

                        for (int i = PageIO.COMMON_HEADER_END + DataPageIO.ITEMS_OFF; i < mem.pageSize(); i++)
                            PageUtils.putByte(pageAddr, i, (byte)0xAB);

                        PageIO.printPage(pageAddr, mem.pageSize());
                    }
                    finally {
                        mem.writeUnlock(fullId.groupId(), fullId.pageId(), page, null, true);
                    }
                }
                finally {
                    mem.releasePage(fullId.groupId(), fullId.pageId(), page);
                }
            }

            wal.fsync(null);
        }
        finally {
            ig.context().cache().context().database().checkpointReadUnlock();
            stopAllGrids(false);
        }

        ig = startGrid(0);

        ig.active(true);

        shared = ig.context().cache().context();

        dbMgr = (GridCacheDatabaseSharedManager)shared.database();

        dbMgr.enableCheckpoints(false);

        wal = shared.wal();

        try (WALIterator it = wal.replay(start)) {
            it.nextX();

            for (FullPageId initialWrite : initWrites) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

                assertTrue(String.valueOf(tup.get2()), tup.get2() instanceof PageSnapshot);

                PageSnapshot snap = (PageSnapshot)tup.get2();

                FullPageId actual = snap.fullPageId();

                //there are extra tracking pages, skip them
                if (TrackingPageIO.VERSIONS.latest().trackingPageFor(actual.pageId(), mem.pageSize()) == actual.pageId()) {
                    tup = it.nextX();

                    assertTrue(tup.get2() instanceof PageSnapshot);

                    actual = ((PageSnapshot)tup.get2()).fullPageId();
                }

                assertEquals(initialWrite, actual);
            }
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testDataWalEntries() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.active(true);

        GridCacheSharedContext<Object, Object> sharedCtx = ig.context().cache().context();
        GridCacheContext<Object, Object> cctx = sharedCtx.cache().cache(cacheName).context();

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)sharedCtx.database();
        IgniteWriteAheadLogManager wal = sharedCtx.wal();

        assertTrue(wal.isAlwaysWriteFullPages());

        db.enableCheckpoints(false).get();

        final int cnt = 10;

        List<DataEntry> entries = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++) {
            GridCacheOperation op = i % 2 == 0 ? GridCacheOperation.UPDATE : GridCacheOperation.DELETE;

            KeyCacheObject key = cctx.toCacheKeyObject(i);

            CacheObject val = null;

            if (op != GridCacheOperation.DELETE)
                val = cctx.toCacheObject("value-" + i);

            entries.add(new DataEntry(cctx.cacheId(), key, val, op, null, cctx.versions().next(), 0L,
                cctx.affinity().partition(i), i));
        }

        UUID cpId = UUID.randomUUID();

        WALPointer start = wal.log(new CheckpointRecord(cpId, null));

        wal.fsync(start);

        for (DataEntry entry : entries)
            wal.log(new DataRecord(entry));

        // Data will not be written to the page store.
        stopAllGrids();

        ig = startGrid(0);

        ig.active(true);

        sharedCtx = ig.context().cache().context();
        cctx = sharedCtx.cache().cache(cacheName).context();

        db = (GridCacheDatabaseSharedManager)sharedCtx.database();
        wal = sharedCtx.wal();

        db.enableCheckpoints(false).get();

        try (WALIterator it = wal.replay(start)) {
            IgniteBiTuple<WALPointer, WALRecord> cpRecordTup = it.nextX();

            assert cpRecordTup.get2() instanceof CheckpointRecord;

            assertEquals(start, cpRecordTup.get1());

            CheckpointRecord cpRec = (CheckpointRecord)cpRecordTup.get2();

            assertEquals(cpId, cpRec.checkpointId());
            assertNull(cpRec.checkpointMark());
            assertFalse(cpRec.end());

            int idx = 0;
            CacheObjectContext coctx = cctx.cacheObjectContext();

            while (idx < entries.size()) {
                IgniteBiTuple<WALPointer, WALRecord> dataRecTup = it.nextX();

                assert dataRecTup.get2() instanceof DataRecord;

                DataRecord dataRec = (DataRecord)dataRecTup.get2();

                DataEntry entry = entries.get(idx);

                assertEquals(1, dataRec.writeEntries().size());

                DataEntry readEntry = dataRec.writeEntries().get(0);

                assertEquals(entry.cacheId(), readEntry.cacheId());
                assertEquals(entry.key().<Integer>value(coctx, true), readEntry.key().<Integer>value(coctx, true));
                assertEquals(entry.op(), readEntry.op());

                if (entry.op() == GridCacheOperation.UPDATE)
                    assertEquals(entry.value().value(coctx, true), readEntry.value().value(coctx, true));
                else
                    assertNull(entry.value());

                assertEquals(entry.writeVersion(), readEntry.writeVersion());
                assertEquals(entry.nearXidVersion(), readEntry.nearXidVersion());
                assertEquals(entry.partitionCounter(), readEntry.partitionCounter());

                idx++;
            }
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testPageWalEntries() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.active(true);

        GridCacheSharedContext<Object, Object> sharedCtx = ig.context().cache().context();
        int cacheId = sharedCtx.cache().cache(cacheName).context().cacheId();

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)sharedCtx.database();
        PageMemory pageMem = sharedCtx.database().memoryPolicy(null).pageMemory();
        IgniteWriteAheadLogManager wal = sharedCtx.wal();

        db.enableCheckpoints(false).get();

        int pageCnt = 100;

        List<FullPageId> pageIds = new ArrayList<>();

        for (int i = 0; i < pageCnt; i++) {
            db.checkpointReadLock();
            try {
                pageIds.add(new FullPageId(pageMem.allocatePage(cacheId, PageIdAllocator.INDEX_PARTITION,
                    PageIdAllocator.FLAG_IDX), cacheId));
            }
            finally {
                db.checkpointReadUnlock();
            }
        }

        UUID cpId = UUID.randomUUID();

        WALPointer start = wal.log(new CheckpointRecord(cpId, null));

        wal.fsync(start);

        ig.context().cache().context().database().checkpointReadLock();

        try {
            for (FullPageId pageId : pageIds)
                writePageData(pageId, pageMem);
        }
        finally {
            ig.context().cache().context().database().checkpointReadUnlock();
        }

        // Data will not be written to the page store.
        stopAllGrids();

        ig = startGrid(0);

        ig.active(true);

        sharedCtx = ig.context().cache().context();

        db = (GridCacheDatabaseSharedManager)sharedCtx.database();
        wal = sharedCtx.wal();

        db.enableCheckpoints(false);

        try (WALIterator it = wal.replay(start)) {
            IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

            assert tup.get2() instanceof CheckpointRecord : tup.get2();

            assertEquals(start, tup.get1());

            CheckpointRecord cpRec = (CheckpointRecord)tup.get2();

            assertEquals(cpId, cpRec.checkpointId());
            assertNull(cpRec.checkpointMark());
            assertFalse(cpRec.end());

            int idx = 0;

            while (idx < pageIds.size()) {
                tup = it.nextX();

                assert tup.get2() instanceof PageSnapshot : tup.get2().getClass();

                PageSnapshot snap = (PageSnapshot)tup.get2();

                //there are extra tracking pages, skip them
                if (TrackingPageIO.VERSIONS.latest().trackingPageFor(snap.fullPageId().pageId(), pageMem.pageSize()) == snap.fullPageId().pageId()) {
                    tup = it.nextX();

                    assertTrue(tup.get2() instanceof PageSnapshot);

                    snap = (PageSnapshot)tup.get2();
                }

                assertEquals(pageIds.get(idx), snap.fullPageId());

                idx++;
            }
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testDirtyFlag() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.active(true);

        GridCacheSharedContext<Object, Object> shared = ig.context().cache().context();

        int cacheId = shared.cache().cache(cacheName).context().cacheId();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)shared.database();

        // Disable integrated checkpoint thread.
        dbMgr.enableCheckpoints(false);

        PageMemoryEx mem = (PageMemoryEx) dbMgr.memoryPolicy(null).pageMemory();

        ig.context().cache().context().database().checkpointReadLock();

        FullPageId[] pageIds = new FullPageId[100];

        try {
            for (int i = 0; i < pageIds.length; i++)
                pageIds[i] = new FullPageId(mem.allocatePage(cacheId, 0, PageIdAllocator.FLAG_DATA), cacheId);

            for (FullPageId fullId : pageIds) {
                long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

                try {
                    assertTrue(mem.isDirty(fullId.groupId(), fullId.pageId(), page)); //page is dirty right after allocation

                    long pageAddr = mem.writeLock(fullId.groupId(), fullId.pageId(), page);

                    PageIO.setPageId(pageAddr, fullId.pageId());

                    try {
                        assertTrue(mem.isDirty(fullId.groupId(), fullId.pageId(), page));
                    }
                    finally {
                        mem.writeUnlock(fullId.groupId(), fullId.pageId(),page, null,true);
                    }

                    assertTrue(mem.isDirty(fullId.groupId(), fullId.pageId(), page));
                }
                finally {
                    mem.releasePage(fullId.groupId(), fullId.pageId(), page);
                }
            }
        }
        finally {
            ig.context().cache().context().database().checkpointReadUnlock();
        }

        Collection<FullPageId> cpPages = mem.beginCheckpoint();

        ig.context().cache().context().database().checkpointReadLock();

        try {
            for (FullPageId fullId : pageIds) {
                assertTrue(cpPages.contains(fullId));

                ByteBuffer buf = ByteBuffer.allocate(mem.pageSize());

                long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

                try {
                    assertTrue(mem.isDirty(fullId.groupId(), fullId.pageId(), page));

                    long pageAddr = mem.writeLock(fullId.groupId(), fullId.pageId(), page);

                    try {
                        assertFalse(mem.isDirty(fullId.groupId(), fullId.pageId(), page));

                        for (int i = PageIO.COMMON_HEADER_END; i < mem.pageSize(); i++)
                            PageUtils.putByte(pageAddr, i, (byte)1);
                    }
                    finally {
                        mem.writeUnlock(fullId.groupId(), fullId.pageId(), page, null, true);
                    }

                    assertTrue(mem.isDirty(fullId.groupId(), fullId.pageId(), page));

                    buf.rewind();

                    mem.getForCheckpoint(fullId, buf, null);

                    buf.position(PageIO.COMMON_HEADER_END);

                    while (buf.hasRemaining())
                        assertEquals((byte)0, buf.get());
                }
                finally {
                    mem.releasePage(fullId.groupId(), fullId.pageId(), page);
                }
            }
        }
        finally {
            ig.context().cache().context().database().checkpointReadUnlock();
        }

        mem.finishCheckpoint();

        for (FullPageId fullId : pageIds) {
            long page = mem.acquirePage(fullId.groupId(), fullId.pageId());
            try {
                assertTrue(mem.isDirty(fullId.groupId(), fullId.pageId(), page));
            }
            finally {
                mem.releasePage(fullId.groupId(), fullId.pageId(), page);
            }
        }
    }

    /**
     * @throws Exception if failed.
     */
    private void writePageData(FullPageId fullId, PageMemory mem) throws Exception {
        long page = mem.acquirePage(fullId.groupId(), fullId.pageId());
        try {
            long pageAddr = mem.writeLock(fullId.groupId(), fullId.pageId(), page);

            try {
                DataPageIO.VERSIONS.latest().initNewPage(pageAddr, fullId.pageId(), mem.pageSize());

                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = PageIO.COMMON_HEADER_END; i < mem.pageSize(); i++)
                    PageUtils.putByte(pageAddr, i, (byte)rnd.nextInt(255));
            }
            finally {
                mem.writeUnlock(fullId.groupId(), fullId.pageId(), page, null, true);
            }
        }
        finally {
            mem.releasePage(fullId.groupId(), fullId.pageId(), page);
        }
    }

    /**
     * @param res Result map to verify.
     * @param mem Memory.
     */
    private void verifyReads(
        Map<FullPageId, Integer> res,
        PageMemory mem,
        WALPointer start,
        IgniteWriteAheadLogManager wal
    ) throws Exception {
        Map<FullPageId, byte[]> replay = new HashMap<>();

        try (WALIterator it = wal.replay(start)) {
            IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

            assertTrue("Invalid record: " + tup, tup.get2() instanceof CheckpointRecord);

            CheckpointRecord cpRec = (CheckpointRecord)tup.get2();

            while (it.hasNextX()) {
                tup = it.nextX();

                WALRecord rec = tup.get2();

                if (rec instanceof CheckpointRecord) {
                    CheckpointRecord end = (CheckpointRecord)rec;

                    // Found the finish mark.
                    if (end.checkpointId().equals(cpRec.checkpointId()) && end.end())
                        break;
                }
                else if (rec instanceof PageSnapshot) {
                    PageSnapshot page = (PageSnapshot)rec;

                    replay.put(page.fullPageId(), page.pageData());
                }
            }
        }

        // Check read-through from the file store.
        for (Map.Entry<FullPageId, Integer> entry : res.entrySet()) {
            FullPageId fullId = entry.getKey();
            int state = entry.getValue();

            if (state == -1) {
                info("Page was never written: " + fullId);

                continue;
            }

            byte[] walData = replay.get(fullId);

            assertNotNull("Missing WAL record for a written page: " + fullId, walData);

            long page = mem.acquirePage(fullId.groupId(), fullId.pageId());
            try {
                long pageAddr = mem.readLock(fullId.groupId(), fullId.pageId(), page);

                try {
                    for (int i = PageIO.COMMON_HEADER_END; i < mem.pageSize(); i++) {
                        assertEquals("Invalid state [pageId=" + fullId + ", pos=" + i + ']',
                            state & 0xFF, PageUtils.getByte(pageAddr, i) & 0xFF);

                        assertEquals("Invalid WAL state [pageId=" + fullId + ", pos=" + i + ']',
                            state & 0xFF, walData[i] & 0xFF);
                    }
                }
                finally {
                    mem.readUnlock(fullId.groupId(), fullId.pageId(), page);
                }
            }
            finally {
                mem.releasePage(fullId.groupId(), fullId.pageId(), page);
            }
        }
    }

    /**
     * @param mem Memory to use.
     * @param storeMgr Store manager.
     * @param cacheId Cache ID.
     * @return Result map of random operations.
     * @throws Exception If failure occurred.
     */
    private IgniteBiTuple<Map<FullPageId, Integer>, WALPointer> runCheckpointing(
        final IgniteEx ig,
        final PageMemoryImpl mem,
        final IgnitePageStoreManager storeMgr,
        final IgniteWriteAheadLogManager wal,
        final int cacheId
    ) throws Exception {
        final ConcurrentMap<FullPageId, Integer> resMap = new ConcurrentHashMap<>();

        final FullPageId pages[] = new FullPageId[TOTAL_PAGES];

        Set<FullPageId> allocated = new HashSet<>();

        for (int i = 0; i < TOTAL_PAGES; i++) {
            FullPageId fullId = new FullPageId(mem.allocatePage(cacheId, 0, PageIdAllocator.FLAG_DATA), cacheId);

            resMap.put(fullId, -1);

            pages[i] = fullId;

            allocated.add(fullId);
        }

        final AtomicBoolean run = new AtomicBoolean(true);

        // Simulate transaction lock.
        final ReadWriteLock updLock = new ReentrantReadWriteLock();

        // Mark the start position.
        CheckpointRecord cpRec = new CheckpointRecord(null);

        WALPointer start = wal.log(cpRec);

        wal.fsync(start);

        IgniteInternalFuture<Long> updFut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (true) {
                    FullPageId fullId = pages[ThreadLocalRandom.current().nextInt(TOTAL_PAGES)];

                    updLock.readLock().lock();

                    try {
                        if (!run.get())
                            return null;

                        ig.context().cache().context().database().checkpointReadLock();

                        try {
                            long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

                            try {
                                long pageAddr = mem.writeLock(fullId.groupId(), fullId.pageId(), page);

                                PageIO.setPageId(pageAddr, fullId.pageId());

                                try {
                                    int state = resMap.get(fullId);

                                    if (state != -1) {
                                        if (VERBOSE)
                                            info("Verify page [fullId=" + fullId + ", state=" + state +
                                                ", buf=" + pageAddr +
                                                ", bhc=" + U.hexLong(System.identityHashCode(pageAddr)) +
                                                ", page=" + U.hexLong(System.identityHashCode(page)) + ']');

                                        for (int i = PageIO.COMMON_HEADER_END; i < mem.pageSize(); i++)
                                            assertEquals("Verify page failed [fullId=" + fullId +
                                                    ", i=" + i +
                                                    ", state=" + state +
                                                    ", buf=" + pageAddr +
                                                    ", bhc=" + U.hexLong(System.identityHashCode(pageAddr)) +
                                                    ", page=" + U.hexLong(System.identityHashCode(page)) + ']',
                                                state & 0xFF, PageUtils.getByte(pageAddr, i) & 0xFF);
                                    }

                                    state = (state + 1) & 0xFF;

                                    if (VERBOSE)
                                        info("Write page [fullId=" + fullId + ", state=" + state +
                                            ", buf=" + pageAddr +
                                            ", bhc=" + U.hexLong(System.identityHashCode(pageAddr)) +
                                            ", page=" + U.hexLong(System.identityHashCode(page)) + ']');

                                    for (int i = PageIO.COMMON_HEADER_END; i < mem.pageSize(); i++)
                                        PageUtils.putByte(pageAddr, i, (byte)state);

                                    resMap.put(fullId, state);
                                }
                                finally {
                                    mem.writeUnlock(fullId.groupId(), fullId.pageId(),page, null,true);
                                }
                            }
                            finally {
                                mem.releasePage(fullId.groupId(), fullId.pageId(),page);}
                            }
                            finally {
                                ig.context().cache().context().database().checkpointReadUnlock();
                            }
                        }
                        finally {
                            updLock.readLock().unlock();
                        }
                    }
                }
            }, 8, "update-thread");

        int checkpoints = 20;

        while (checkpoints > 0) {
            Map<FullPageId, Integer> snapshot = null;

            Collection<FullPageId> pageIds;

            updLock.writeLock().lock();

            try {
                snapshot = new HashMap<>(resMap);

                pageIds = mem.beginCheckpoint();

                checkpoints--;

                if (checkpoints == 0)
                    // No more writes should be done at this point.
                    run.set(false);

                info("Acquired pages for checkpoint: " + pageIds.size());
            }
            finally {
                updLock.writeLock().unlock();
            }

            boolean ok = false;

            try {
                ByteBuffer tmpBuf = ByteBuffer.allocate(mem.pageSize());

                tmpBuf.order(ByteOrder.nativeOrder());

                long begin = System.currentTimeMillis();

                long cp = 0;

                long write = 0;

                for (FullPageId fullId : pageIds) {
                    long cpStart = System.nanoTime();

                    Integer tag = mem.getForCheckpoint(fullId, tmpBuf, null);

                    if (tag == null)
                        continue;

                    long cpEnd = System.nanoTime();

                    cp += cpEnd - cpStart;

                    Integer state = snapshot.get(fullId);

                    if (allocated.contains(fullId) && state != -1) {
                        tmpBuf.rewind();

                        Integer first = null;

                        for (int i = PageIO.COMMON_HEADER_END; i < mem.pageSize(); i++) {
                            int val = tmpBuf.get(i) & 0xFF;

                            if (first == null)
                                first = val;

                            // Avoid string concat.
                            if (first != val)
                                assertEquals("Corrupted buffer at position [pageId=" + fullId + ", pos=" + i + ']',
                                    (int)first, val);

                            // Avoid string concat.
                            if (state != val)
                                assertEquals("Invalid value at position [pageId=" + fullId + ", pos=" + i + ']',
                                    (int)state, val);
                        }
                    }

                    tmpBuf.rewind();

                    long writeStart = System.nanoTime();

                    storeMgr.write(cacheId, fullId.pageId(), tmpBuf, tag);

                    long writeEnd = System.nanoTime();

                    write += writeEnd - writeStart;

                    tmpBuf.rewind();
                }

                long syncStart = System.currentTimeMillis();

                storeMgr.sync(cacheId, 0);

                long end = System.currentTimeMillis();

                info("Written pages in " + (end - begin) + "ms, copy took " + (cp / 1_000_000) + "ms, " +
                    "write took " + (write / 1_000_000) + "ms, sync took " + (end - syncStart) + "ms");

                ok = true;
            }
            finally {
                info("Finishing checkpoint...");

                mem.finishCheckpoint();

                info("Finished checkpoint");

                if (!ok) {
                    info("Cancelling updates...");

                    run.set(false);

                    updFut.get();
                }
            }

            if (checkpoints != 0)
                Thread.sleep(2_000);
        }

        info("checkpoints=" + checkpoints + ", done=" + updFut.isDone());

        updFut.get();

        assertEquals(0, mem.activePagesCount());

        for (FullPageId fullId : pages) {

            long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

            try {
                assertFalse("Page has a temp heap copy after the last checkpoint: [cacheId=" +
                    fullId.groupId() + ", pageId=" + fullId.pageId() + "]", mem.hasTempCopy(page));

                assertFalse("Page is dirty after the last checkpoint: [cacheId=" +
                    fullId.groupId() + ", pageId=" + fullId.pageId() + "]", mem.isDirty(fullId.groupId(), fullId.pageId(), page));
            }
            finally {
                mem.releasePage(fullId.groupId(), fullId.pageId(), page);
            }
        }

        return F.t((Map<FullPageId, Integer>)resMap, start);
    }

    /**
     *
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }
}
