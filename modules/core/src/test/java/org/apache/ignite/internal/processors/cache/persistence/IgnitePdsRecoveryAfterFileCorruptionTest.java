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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
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
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgnitePdsRecoveryAfterFileCorruptionTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Total pages. */
    private static final int totalPages = 1024;

    /** Cache name. */
    private final String cacheName = "cache";

    /** Policy name. */
    private final String policyName = "dfltMemPlc";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(cacheName);
        ccfg.setAffinity(new RendezvousAffinityFunction(true, 1));

        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);

        cfg.setCacheConfiguration(ccfg);

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName(policyName);
        memPlcCfg.setInitialSize(1024 * 1024 * 1024);
        memPlcCfg.setMaxSize(1024 * 1024 * 1024);

        dbCfg.setMemoryPolicies(memPlcCfg);
        dbCfg.setDefaultMemoryPolicyName(policyName);

        cfg.setMemoryConfiguration(dbCfg);

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration()
                .setCheckpointingFrequency(500)
                .setAlwaysWriteFullPages(true)
        );

        cfg.setDiscoverySpi(
            new TcpDiscoverySpi()
                .setIpFinder(ipFinder)
        );

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
    public void testPageRecoveryAfterFileCorruption() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.active(true);

        IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

        // Put for create data store and init meta page.
        cache.put(1, 1);

        GridCacheSharedContext sharedCtx = ig.context().cache().context();

        GridCacheDatabaseSharedManager psMgr = (GridCacheDatabaseSharedManager)sharedCtx.database();

        FilePageStoreManager pageStore = (FilePageStoreManager)sharedCtx.pageStore();

        U.sleep(1_000);

        // Disable integrated checkpoint thread.
        psMgr.enableCheckpoints(false).get();

        PageMemory mem = sharedCtx.database().memoryPolicy(policyName).pageMemory();

        int cacheId = sharedCtx.cache().cache(cacheName).context().cacheId();

        FullPageId[] pages = new FullPageId[totalPages];

        for (int i = 0; i < totalPages; i++)
            pages[i] = new FullPageId(mem.allocatePage(cacheId, 0, PageIdAllocator.FLAG_DATA), cacheId);

        generateWal(
            (PageMemoryImpl)mem,
            sharedCtx.pageStore(),
            sharedCtx.wal(),
            cacheId,
            pages
        );

        eraseDataFromDisk(pageStore, cacheId, pages[0]);

        stopAllGrids();

        ig = startGrid(0);

        ig.active(true);

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

        FileIO fileIO = U.field(filePageStore, "fileIO");

        long size = fileIO.size();

        fileIO.write(ByteBuffer.allocate((int)size - filePageStore.headerSize()), filePageStore.headerSize());

        fileIO.force();
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
            long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

            try {
                long pageAddr = mem.readLock(fullId.groupId(), fullId.pageId(), page);

                for (int j = PageIO.COMMON_HEADER_END; j < mem.pageSize(); j += 4)
                    assertEquals(j + (int)fullId.pageId(), PageUtils.getInt(pageAddr, j));

                mem.readUnlock(fullId.groupId(), fullId.pageId(), page);
            }
            finally {
                mem.releasePage(fullId.groupId(), fullId.pageId(), page);
            }
        }
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
        CheckpointRecord cpRec = new CheckpointRecord(null);

        WALPointer start = wal.log(cpRec);

        wal.fsync(start);

        for (int i = 0; i < totalPages; i++) {
            FullPageId fullId = pages[i];

            long page = mem.acquirePage(fullId.groupId(), fullId.pageId());

            try {
                long pageAddr = mem.writeLock(fullId.groupId(), fullId.pageId(), page);

                PageIO.setPageId(pageAddr, fullId.pageId());

                try {
                    for (int j = PageIO.COMMON_HEADER_END; j < mem.pageSize(); j += 4)
                        PageUtils.putInt(pageAddr, j, j + (int)fullId.pageId());
                }
                finally {
                    mem.writeUnlock(fullId.groupId(), fullId.pageId(), page, null, true);
                }
            }
            finally {
                mem.releasePage(fullId.groupId(), fullId.pageId(), page);
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

                    Integer tag = mem.getForCheckpoint(fullId, tmpBuf, null);

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

        wal.fsync(wal.log(new CheckpointRecord(null)));

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
    }

    /**
     *
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }
}
