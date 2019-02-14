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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;

/** */
public class IgnitePdsCachePartitonsBackupSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_PARTS_COUNT = 16;

    /** */
    private static final int PAGE_SIZE = 1024;

    /** */
    private static final FileIOFactory ioFactory = new RandomAccessFileIOFactory();

    /** */
    private static final DataStorageConfiguration memCfg = new DataStorageConfiguration()
        .setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024)
                .setPersistenceEnabled(true)
        )
        .setPageSize(PAGE_SIZE)
        .setWalMode(WALMode.LOG_ONLY);

    /** */
    private static final CacheConfiguration<Integer, Integer> defaultCacheCfg =
        new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false)
                .setPartitions(CACHE_PARTS_COUNT));

    /** */
    private FilePageStoreFactory pageStoreFactory = new FileVersionCheckingFactory(ioFactory, ioFactory, memCfg);

    /** Directory to store temporary files on testing cache backup process. */
    private File mergeTempDir;

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
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(memCfg)
            .setCacheConfiguration(defaultCacheCfg);
    }

    /**
     * @throws Exception Exception.
     */
    @Test
    public void testCopyCachePartitonFiles() throws Exception {
        IgniteEx ig0 = startGrid(0);

        ig0.cluster().active(true);

        for (int i = 0; i < 2048; i++)
            ig0.cache(DEFAULT_CACHE_NAME).put(i, i);

        ig0.context().cache().context().database()
            .waitForCheckpoint("save cache data to partition files");

        File cacheWorkDir = ((FilePageStoreManager)ig0.context().cache().context().pageStore())
            .cacheWorkDir(defaultCacheCfg);

        Map<String, Integer> exeptedCacheCRCParts = calculateCRC32Partitions(cacheWorkDir);

        GridCacheSharedContext<?, ?> cctx1 = ig0.context().cache().context();

        File mergeCacheDir = U.resolveWorkDirectory(
            mergeTempDir.getAbsolutePath(),
            cacheDirName(defaultCacheCfg),
            true
        );

        final CountDownLatch slowCopy = new CountDownLatch(1);

        // Run the next checkpoint and produce dirty pages to generate onPageWrite events.
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    for (int i = 2048; i < 4096; i++)
                        ig0.cache(DEFAULT_CACHE_NAME).put(i, i);

                    CheckpointFuture cpFut = cctx1.database().forceCheckpoint("the next one");

                    cpFut.finishFuture().get();

                    slowCopy.countDown();

                    U.log(log, "New checkpoint finished succesfully.");
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        });

        final ByteBuffer pageBuff = ByteBuffer.allocate(PAGE_SIZE)
            .order(ByteOrder.nativeOrder());

        cctx1.storeBackup()
            .backup(
                1,
                CU.cacheId(DEFAULT_CACHE_NAME),
                IntStream.range(0, CACHE_PARTS_COUNT).boxed().collect(Collectors.toSet()),
                new BackupProcessTask() {
                    /** Last seen handled partition id file. */
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

                                assert pageBuff.remaining() == PAGE_SIZE : pageBuff.remaining();

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

        Map<String, Integer> snapshottedCacheParts = calculateCRC32Partitions(mergeCacheDir);

        assertEquals(exeptedCacheCRCParts, snapshottedCacheParts);
    }

    /**
     * Calculate CRC for all partition files of specified cache.
     *
     * @param cacheDir Cache directory to iterate over partition files.
     * @return The map of [fileName, checksum].
     * @throws IOException If fails.
     */
    private static Map<String, Integer> calculateCRC32Partitions(File cacheDir) throws IOException {
        assert cacheDir.isDirectory();

        Map<String, Integer> result = new HashMap<>();

        try (DirectoryStream<Path> partFiles = newDirectoryStream(cacheDir.toPath(),
            p -> p.toFile().getName().startsWith(PART_FILE_PREFIX))
        ) {
            for (Path path : partFiles)
                result.put(path.toFile().getName(), FastCrc.calcCrc(path.toFile()));
        }

        return result;
    }

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
}
