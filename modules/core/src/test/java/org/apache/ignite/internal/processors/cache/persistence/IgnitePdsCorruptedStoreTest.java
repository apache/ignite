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
import java.nio.file.OpenOption;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_SKIP_CRC;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_ID;

/**
 *
 */
public class IgnitePdsCorruptedStoreTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME1 = "cache1";

    /** */
    private static final String CACHE_NAME2 = "cache2";

    /** Failure handler. */
    private DummyFailureHandler failureHnd;

    /** Failing FileIO factory. */
    private FailingFileIOFactory failingFileIOFactory;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConnectorConfiguration(new ConnectorConfiguration());

        cfg.setConsistentId(igniteInstanceName);

        failingFileIOFactory = new FailingFileIOFactory();

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(100 * 1024 * 1024)
                    .setPersistenceEnabled(true)
            )
            .setFileIOFactory(failingFileIOFactory);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME1), cacheConfiguration(CACHE_NAME2));

        failureHnd = new DummyFailureHandler();

        cfg.setFailureHandler(failureHnd);

        return cfg;
    }

    /**
     * @return File or folder in work directory.
     * @throws IgniteCheckedException If failed to resolve file name.
     */
    private File file(String file) throws IgniteCheckedException {
        return U.resolveWorkDirectory(U.defaultWorkDirectory(), file, false);
    }

    /**
     * Create cache configuration.
     *
     * @param name Cache name.
     */
    private CacheConfiguration cacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setName(name);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setBackups(2);

        return ccfg;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testNodeInvalidatedWhenPersistenceIsCorrupted() throws Exception {
        Ignite ignite = startGrid(0);

        startGrid(1);

        ignite.cluster().active(true);

        awaitPartitionMapExchange();

        IgniteCache<Integer, String> cache1 = ignite.cache(CACHE_NAME1);

        for (int i = 0; i < 100; ++i)
            cache1.put(i, String.valueOf(i));

        forceCheckpoint();

        cache1.put(2, "test");

        String nodeName = ignite.name().replaceAll("\\.", "_");

        stopAllGrids();

        U.delete(file(String.format("db/%s/cache-%s/part-2.bin", nodeName, CACHE_NAME1)));

        startGrid(1);

        try {
            startGrid(0);
        }
        catch (IgniteCheckedException ex) {
            throw ex;
        }

        waitFailure(StorageException.class);
    }

    /**
     * Test node invalidation when page CRC is wrong and page not found in wal.
     *
     * @throws Exception In case of fail
     */
    public void testWrongPageCRC() throws Exception {
        System.setProperty(IGNITE_PDS_SKIP_CRC, "true");

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        ignite.cluster().active(false);

        stopGrid(0);

        System.setProperty(IGNITE_PDS_SKIP_CRC, "false");

        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);
        File walDir = new File(dbDir, "wal");

        U.delete(walDir);

        try {
            startGrid(0);

            ignite.cluster().active(true);
        }
        catch (Exception e) {
            // No-op.
        }

        waitFailure(StorageException.class);
    }

    /**
     * Test node invalidation when meta storage is corrupted.
     */
    public void testMetaStorageCorruption() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        MetaStorage metaStorage = ignite.context().cache().context().database().metaStorage();

        corruptTreeRoot(ignite, (PageMemoryEx)metaStorage.pageMemory(), METASTORAGE_CACHE_ID, 0);

        stopGrid(0);

        try {
            startGrid(0);

            ignite.cluster().active(true);
        }
        catch (Exception e) {
            // No-op.
        }

        waitFailure(StorageException.class);
    }

    /**
     * Test node invalidation when cache meta is corrupted.
     */
    public void testCacheMetaCorruption() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteInternalCache cache = ignite.cachex(CACHE_NAME1);

        cache.put(1, 1);

        int partId = cache.affinity().partition(1);

        int grpId = cache.context().group().groupId();

        corruptTreeRoot(ignite, (PageMemoryEx)cache.context().dataRegion().pageMemory(), grpId, partId);

        ignite.cluster().active(false);

        stopGrid(0);

        try {
            startGrid(0);

            ignite.cluster().active(true);

            cache.put(1, 1);
        }
        catch (Exception e) {
            // No-op.
        }

        waitFailure(StorageException.class);
    }

    /**
     * @param ignite Ignite.
     * @param grpId Group id.
     * @param partId Partition id.
     */
    private void corruptTreeRoot(IgniteEx ignite, PageMemoryEx pageMem, int grpId, int partId)
        throws IgniteCheckedException {
        ignite.context().cache().context().database().checkpointReadLock();

        try {
            long partMetaId = pageMem.partitionMetaPageId(grpId, partId);
            long partMetaPage = pageMem.acquirePage(grpId, partMetaId);

            try {
                long pageAddr = pageMem.writeLock(grpId, partMetaId, partMetaPage);

                try {
                    PagePartitionMetaIO io = PageIO.getPageIO(pageAddr);

                    // Corrupt tree root
                    io.setTreeRoot(pageAddr, PageIdUtils.pageId(0, (byte)0, 0));
                }
                catch (Exception e) {
                    fail("Failed to change page: " + e.getMessage());
                }
                finally {
                    pageMem.writeUnlock(grpId, partMetaId, partMetaPage, null, true);
                }
            }
            finally {
                pageMem.releasePage(grpId, partMetaId, partMetaPage);
            }
        }
        finally {
            ignite.context().cache().context().database().checkpointReadUnlock();
        }
    }

    /**
     * Test node invalidation when meta store is read only.
     */
    public void testReadOnlyMetaStore() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteInternalCache cache = ignite0.cachex(CACHE_NAME1);

        cache.put(1, 1);

        ignite0.cluster().active(false);

        FilePageStoreManager storeMgr = ((FilePageStoreManager)ignite0.context().cache().context().pageStore());

        File workDir = storeMgr.workDir();
        File metaStoreDir = new File(workDir, MetaStorage.METASTORAGE_CACHE_NAME.toLowerCase());
        File metaStoreFile = new File(metaStoreDir, String.format(FilePageStoreManager.PART_FILE_TEMPLATE, 0));

        metaStoreFile.setWritable(false);

        try {
            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        ignite0.cluster().active(true);
                    }
                    catch (Exception ignore) {
                        // No-op.
                    }
                }
            });

            waitFailure(IOException.class);

            fut.cancel();
        }
        finally {
            metaStoreFile.setWritable(true);
        }
    }


    /**
     * Test node invalidation due to checkpoint error.
     */
    public void testCheckpointFailure() throws Exception {
        IgniteEx ignite = startGrid(0);

        failingFileIOFactory.createClosure(new IgniteBiClosure<File, OpenOption[], FileIO>() {
            @Override public FileIO apply(File file, OpenOption[] options) {
                if (file.getName().indexOf("-END.bin") >= 0) {
                    FileIO delegate;

                    try {
                        delegate = failingFileIOFactory.delegateFactory().create(file, options);
                    }
                    catch (IOException ignore) {
                        return null;
                    }

                    return new FileIODecorator(delegate) {
                        @Override public void close() throws IOException {
                            throw new IOException("Checkpoint failed");
                        }
                    };
                }

                return null;
            }
        });

        ignite.cluster().active(true);

        try {
            forceCheckpoint(ignite);
        }
        catch (Exception ignore) {
            // No-op.
        }

        waitFailure(IOException.class);
    }

    /**
     * @param expError Expected error.
     */
    private void waitFailure(Class<? extends Throwable> expError) throws IgniteInterruptedCheckedException {
        assertTrue(GridTestUtils.waitForCondition(() -> failureHnd.failure(), 5_000L));

        assertTrue(X.hasCause(failureHnd.error(), expError));
    }

    /**
     * Dummy failure handler
     */
    public static class DummyFailureHandler implements FailureHandler {
        /** Failure. */
        private volatile boolean failure = false;

        /** Error. */
        private volatile Throwable error = null;

        /**
         * @return failure.
         */
        public boolean failure() {
            return failure;
        }

        /**
         * @return Error.
         */
        public Throwable error() {
            return error;
        }

        /** {@inheritDoc} */
        @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
            failure = true;
            error = failureCtx.error();

            return true;
        }
    }

    /**
     * Create File I/O which can fail according to implemented closure.
     */
    private static class FailingFileIOFactory implements FileIOFactory {
        /** Delegate factory. */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /** Create FileIO closure. */
        private volatile IgniteBiClosure<File, OpenOption[], FileIO> createClo;

        /** {@inheritDoc} */
        @Override public FileIO create(File file) throws IOException {
            return create(file, CREATE, READ, WRITE);
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... openOption) throws IOException {
            FileIO fileIO = null;
            if (createClo != null)
                fileIO = createClo.apply(file, openOption);

            return fileIO != null ? fileIO : delegateFactory.create(file, openOption);
        }

        /**
         * @param createClo FileIO create closure.
         */
        public void createClosure(IgniteBiClosure<File, OpenOption[], FileIO> createClo) {
            this.createClo = createClo;
        }

        /**
         *
         */
        public FileIOFactory delegateFactory() {
            return delegateFactory;
        }
    }
}
