package org.apache.ignite.cache.database;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistenceConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.database.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.database.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.database.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.database.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgnitePersistentStoreRecoveryAfterFileCorruptionTest extends GridCommonAbstractTest {
    /** Total pages. */
    private static final int totalPages = 1024;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(FileWriteAheadLogManager.IGNITE_PDS_WAL_ALWAYS_WRITE_FULL_PAGES, "true");

        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();

        System.clearProperty(FileWriteAheadLogManager.IGNITE_PDS_WAL_ALWAYS_WRITE_FULL_PAGES);
    }

    /**
     * @throws Exception if failed.
     */
    public void testPageRecoveryAfterFileCorruption() throws Exception {
        fail(); //todo @Ed

        IgniteEx ig = startGrid(0);

        GridCacheSharedContext<Object, Object> shared = ig.context().cache().context();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)shared.database();

        IgnitePageStoreManager pageStore = shared.pageStore();

        U.sleep(1_000);

        // Disable integrated checkpoint thread.
        dbMgr.enableCheckpoints(false).get();

        PageMemory mem = shared.database().memoryPolicy(null).pageMemory();

        int cacheId = shared.cache().cache("partitioned").context().cacheId();

        FullPageId[] pages = new FullPageId[totalPages];

        for (int i = 0; i < totalPages; i++) {
            FullPageId fullId = new FullPageId(mem.allocatePage(cacheId, 0, PageIdAllocator.FLAG_DATA), cacheId);

            pages[i] = fullId;
        }

        generateWal(
            (PageMemoryImpl)mem,
            pageStore,
            shared.wal(),
            cacheId,
            pages
        );

        eraseDataFromDisk((FilePageStoreManager)pageStore, cacheId, pages[0]);

        stopAllGrids();

        ig = startGrid(0);

        checkRestore(ig, pages);
    }

    /**
     * @param pageStore Page store.
     * @param cacheId Cache id.
     * @param page Page.
     */
    private void eraseDataFromDisk(
        FilePageStoreManager pageStore,
        int cacheId,
        FullPageId page
    ) throws IgniteCheckedException, IOException {
        PageStore store = pageStore.getStore(
            cacheId,
            PageIdUtils.partId(page.pageId())
        );

        FilePageStore filePageStore = (FilePageStore)store;

        FileChannel ch = U.field(filePageStore, "ch");

        long size = ch.size();

        ch.write(ByteBuffer.allocate((int)size - FilePageStore.HEADER_SIZE), FilePageStore.HEADER_SIZE);

        ch.force(false);
    }

    /**
     * @param ig Ig.
     * @param pages Pages.
     */
    private void checkRestore(IgniteEx ig, FullPageId[] pages) throws IgniteCheckedException {
        GridCacheSharedContext<Object, Object> shared = ig.context().cache().context();

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)shared.database();

        dbMgr.enableCheckpoints(false).get();

        PageMemory mem = shared.database().memoryPolicy(null).pageMemory();

        for (FullPageId fullId : pages) {
            long page = mem.acquirePage(fullId.cacheId(), fullId.pageId());

            try {
                long pageAddr = mem.readLock(fullId.cacheId(), fullId.pageId(), page);

                for (int j = PageIO.COMMON_HEADER_END; j < mem.pageSize(); j += 4)
                    assertEquals(j + (int)fullId.pageId(), PageUtils.getInt(pageAddr, j));

                mem.readUnlock(fullId.cacheId(), fullId.pageId(), page);
            }
            finally {
                mem.releasePage(fullId.cacheId(), fullId.pageId(), page);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration("partitioned");

        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);

        cfg.setCacheConfiguration(ccfg);

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setSize(1024 * 1024 * 1024);

        dbCfg.setMemoryPolicies(memPlcCfg);
        dbCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(dbCfg);

        PersistenceConfiguration pCfg = new PersistenceConfiguration();

        pCfg.setCheckpointFrequency(500);

        cfg.setPersistenceConfiguration(pCfg);

        return cfg;
    }

    /**
     * @param mem Mem.
     * @param storeMgr Store manager.
     * @param wal Wal.
     * @param cacheId Cache id.
     * @param pages Pages.
     */
    private void generateWal(
        final PageMemoryImpl mem,
        final IgnitePageStoreManager storeMgr,
        final IgniteWriteAheadLogManager wal,
        final int cacheId, FullPageId[] pages
    ) throws Exception {
        // Mark the start position.
        CheckpointRecord cpRec = new CheckpointRecord(null, false);

        WALPointer start = wal.log(cpRec);

        wal.fsync(start);

        for (int i = 0; i < totalPages; i++) {
            FullPageId fullId = pages[i];

            long page = mem.acquirePage(fullId.cacheId(), fullId.pageId());

            try {
                long pageAddr = mem.writeLock(fullId.cacheId(), fullId.pageId(), page);

                PageIO.setPageId(pageAddr, fullId.pageId());

                try {
                    for (int j = PageIO.COMMON_HEADER_END; j < mem.pageSize(); j += 4)
                        PageUtils.putInt(pageAddr, j, j + (int)fullId.pageId());
                }
                finally {
                    mem.writeUnlock(fullId.cacheId(), fullId.pageId(), page, null, true);
                }
            }
            finally {
                mem.releasePage(fullId.cacheId(), fullId.pageId(), page);
            }
        }

        Collection<FullPageId> pageIds = mem.beginCheckpoint();

        info("Acquired pages for checkpoint: " + pageIds.size());

        try {
            ByteBuffer tmpBuf = ByteBuffer.allocate(mem.pageSize());

            tmpBuf.order(ByteOrder.nativeOrder());

            long begin = System.currentTimeMillis();

            long cp = 0;

            long write = 0;

            for (int i = 0; i < totalPages; i++) {
                FullPageId fullId = pages[i];

                if (pageIds.contains(fullId)) {
                    long cpStart = System.nanoTime();

                    Integer tag = mem.getForCheckpoint(fullId, tmpBuf);

                    if (tag == null)
                        continue;

                    long cpEnd = System.nanoTime();

                    cp += cpEnd - cpStart;
                    tmpBuf.rewind();

                    for (int j = PageIO.COMMON_HEADER_END; j < mem.pageSize(); j += 4)
                        assertEquals(j + (int)fullId.pageId(), tmpBuf.getInt(j));

                    tmpBuf.rewind();

                    long writeStart = System.nanoTime();

                    storeMgr.write(cacheId, fullId.pageId(), tmpBuf, tag);

                    long writeEnd = System.nanoTime();

                    write += writeEnd - writeStart;

                    tmpBuf.rewind();
                }
            }

            long syncStart = System.currentTimeMillis();

            storeMgr.sync(cacheId, 0);

            long end = System.currentTimeMillis();

            info("Written pages in " + (end - begin) + "ms, copy took " + (cp / 1_000_000) + "ms, " +
                "write took " + (write / 1_000_000) + "ms, sync took " + (end - syncStart) + "ms");
        }
        finally {
            info("Finishing checkpoint...");

            mem.finishCheckpoint();

            info("Finished checkpoint");
        }

        wal.fsync(wal.log(new CheckpointRecord(null, false)));

        for (FullPageId fullId : pages) {
            long page = mem.acquirePage(fullId.cacheId(), fullId.pageId());

            try {
                assertFalse("Page has a temp heap copy after the last checkpoint: [cacheId=" +
                    fullId.cacheId() + ", pageId=" + fullId.pageId() + "]", mem.hasTempCopy(page));

                assertFalse("Page is dirty after the last checkpoint: [cacheId=" +
                    fullId.cacheId() + ", pageId=" + fullId.pageId() + "]", mem.isDirty(fullId.cacheId(), fullId.pageId(), page));
            }
            finally {
                mem.releasePage(fullId.cacheId(), fullId.pageId(), page);
            }
        }
    }

    /**
     *
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }
}
