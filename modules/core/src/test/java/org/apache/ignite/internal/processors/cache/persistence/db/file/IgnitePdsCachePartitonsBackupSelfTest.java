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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageStoreWriteHandler;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.AllocatedPageTracker;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.backup.BackupProcessTask;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;

/** */
public class IgnitePdsCachePartitonsBackupSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_PARTS_COUNT = 16;

    /** */
    private static final FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** */
    private static final DataStorageConfiguration memCfg = new DataStorageConfiguration()
        .setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024)
                .setPersistenceEnabled(true)
        )
        .setPageSize(1024)
        .setWalMode(WALMode.LOG_ONLY);

    /** */
    private FilePageStoreFactory pageStoreFactory = new FileVersionCheckingFactory(ioFactory, ioFactory, memCfg);

    /** Directory to store temporary files on testing cache backup process. */
    private File mergeTempDir;

    /**
     * @param from File to copy from.
     * @param offset Starting file position.
     * @param count Bytes to copy to destination.
     * @param to Output directory.
     * @throws IgniteCheckedException If fails.
     */
    private static File copy(File from, long offset, long count, File to) throws IgniteCheckedException {
        assert to.isDirectory();

        try {
            File destFile = new File(to, from.getName());

            if (!destFile.exists() || destFile.delete())
                destFile.createNewFile();

            try (FileChannel src = new FileInputStream(from).getChannel();
                 FileChannel dest = new FileOutputStream(destFile).getChannel()) {
                src.position(offset);

                long written = 0;

                while (written < count)
                    written += src.transferTo(written, count - written, dest);
            }

            return destFile;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** */
    @Before
    public void beforeTestBackup() throws Exception {
        cleanPersistenceDir();

        mergeTempDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "merge", true);
    }

    /** */
    @After
    public void afterTestBackup() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false)
                .setPartitions(CACHE_PARTS_COUNT));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception Exception.
     */
    @Test
    public void testCopyCachePartitonFiles() throws Exception {
        final IgniteEx ignite0 = (IgniteEx)startGrid(1);

        ignite0.cluster().active(true);

        final IgniteCache<Integer, Integer> cache = ignite0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 2048; i++)
            cache.put(i, i);

        GridCacheSharedContext<?, ?> cctx0 = ignite0.context().cache().context();

        File mergeCacheDir = U.resolveWorkDirectory(mergeTempDir.getAbsolutePath(),
            cacheDirName(cctx0.cache().cacheGroup(CU.cacheId(DEFAULT_CACHE_NAME)).config()), true);

        final CountDownLatch slowCopy = new CountDownLatch(1);

        // Run the next checkpoint and produce dirty pages to generate onPageWrite events.
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    for (int i = 2048; i < 4096; i++)
                        cache.put(i, i);

                    CheckpointFuture cpFut = cctx0.database().forceCheckpoint("the next one");

                    cpFut.finishFuture().get();

                    slowCopy.countDown();

                    U.log(log, "New checkpoint finished succesfully.");
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        });

        final int pageSize = ignite0.cachex(DEFAULT_CACHE_NAME)
            .context()
            .dataRegion()
            .pageMemory()
            .pageSize();

        final ByteBuffer pageBuff = ByteBuffer.allocate(pageSize)
            .order(ByteOrder.nativeOrder());

        cctx0.storeBackup()
            .backup(
                1,
                CU.cacheId(DEFAULT_CACHE_NAME),
                IntStream.range(0, CACHE_PARTS_COUNT).boxed().collect(Collectors.toSet()),
                new BackupProcessTask() {
                    /** Saved partition id file. */
                    private File lastSavedPartId;

                    @Override public void handlePartition(
                        GroupPartitionId grpPartId,
                        File file,
                        long offset,
                        long count
                    ) throws IgniteCheckedException {
                        try {
                            slowCopy.await();

                            lastSavedPartId = copy(file, offset, count, mergeCacheDir);
                        }
                        catch (InterruptedException e) {
                            throw new IgniteCheckedException(e);
                        }
                    }

                    @Override public void handleDelta(
                        GroupPartitionId grpPartId,
                        File file,
                        long offset,
                        long count
                    ) throws IgniteCheckedException {
                        // Will perform a copy delta file page by page simultaneously with merge pages operation.
                        pageBuff.clear();

                        try (SeekableByteChannel src = Files.newByteChannel(file.toPath())) {
                            src.position(offset);

                            PageStore pageStore = pageStoreFactory.createPageStore(
                                PageMemory.FLAG_DATA,
                                lastSavedPartId,
                                AllocatedPageTracker.NO_OP,
                                PageStoreWriteHandler.NO_OP
                            );

                            long readed = offset;

                            while ((readed += src.read(pageBuff)) > 0 && readed < count) {
                                pageBuff.rewind();

                                assert pageBuff.remaining() == pageSize : pageBuff.remaining();

                                long pageId = PageIO.getPageId(pageBuff);

                                pageStore.write(pageId, pageBuff, 0, true);

                                pageBuff.flip();
                            }
                        }
                        catch (IOException e) {
                            throw new IgniteCheckedException(e);
                        }
                    }
                });
    }
}
