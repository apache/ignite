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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardCopyOption;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.GridStripedReadWriteLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;

/**
 * File page store manager.
 */
public class FilePageStoreManager extends GridCacheSharedManagerAdapter implements IgnitePageStoreManager {
    /** File suffix. */
    public static final String FILE_SUFFIX = ".bin";

    /** Suffix for zip files */
    public static final String ZIP_SUFFIX = ".zip";

    /** Suffix for tmp files */
    public static final String TMP_SUFFIX = ".tmp";

    /** Partition file prefix. */
    public static final String PART_FILE_PREFIX = "part-";

    /** */
    public static final String INDEX_FILE_NAME = "index" + FILE_SUFFIX;

    /** */
    public static final String PART_FILE_TEMPLATE = PART_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** */
    public static final String CACHE_DIR_PREFIX = "cache-";

    /** */
    public static final String CACHE_GRP_DIR_PREFIX = "cacheGroup-";

    /** */
    public static final String CACHE_DATA_FILENAME = "cache_data.dat";

    /** */
    public static final String CACHE_DATA_TMP_FILENAME = CACHE_DATA_FILENAME + TMP_SUFFIX;

    /** */
    public static final String DFLT_STORE_DIR = "db";

    /** */
    public static final String META_STORAGE_NAME = "metastorage";

    /** Matcher for searching of *.tmp files. */
    public static final PathMatcher TMP_FILE_MATCHER =
        FileSystems.getDefault().getPathMatcher("glob:**" + TMP_SUFFIX);

    /** Listeners of configuration changes e.g. overwrite or remove actions. */
    private final List<BiConsumer<String, File>> lsnrs = new CopyOnWriteArrayList<>();

    /** Lock which guards configuration changes. */
    private final ReentrantReadWriteLock chgLock = new ReentrantReadWriteLock();

    /** Marshaller. */
    private final Marshaller marshaller;

    /**
     * Executor to disallow running code that modifies data in idxCacheStores concurrently with cleanup of file page
     * store.
     */
    private final LongOperationAsyncExecutor cleanupAsyncExecutor;

    /** */
    private final Map<Integer, CacheStoreHolder> idxCacheStores;

    /** */
    private final IgniteConfiguration igniteCfg;

    /**
     * File IO factory for page store, by default is taken from {@link #dsCfg}.
     * May be overriden by block read/write.
     */
    private FileIOFactory pageStoreFileIoFactory;

    /**
     * File IO factory for page store V1 and for fast checking page store (non block read).
     * By default is taken from {@link #dsCfg}.
     */
    private FileIOFactory pageStoreV1FileIoFactory;

    /** */
    private final DataStorageConfiguration dsCfg;

    /** Absolute directory for file page store. Includes consistent id based folder. */
    private File storeWorkDir;

    /** */
    private final long metaPageId = PageIdUtils.pageId(-1, PageMemory.FLAG_IDX, 0);

    /** */
    private final Set<Integer> grpsWithoutIdx = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    /** */
    private final GridStripedReadWriteLock initDirLock =
        new GridStripedReadWriteLock(Math.max(Runtime.getRuntime().availableProcessors(), 8));

    /**
     * @param ctx Kernal context.
     */
    public FilePageStoreManager(GridKernalContext ctx) {
        igniteCfg = ctx.config();

        cleanupAsyncExecutor =
            new LongOperationAsyncExecutor(ctx.igniteInstanceName(), ctx.config().getGridLogger());

        idxCacheStores = new IdxCacheStores<>(cleanupAsyncExecutor);

        DataStorageConfiguration dsCfg = igniteCfg.getDataStorageConfiguration();

        assert dsCfg != null;

        this.dsCfg = dsCfg;

        pageStoreV1FileIoFactory = pageStoreFileIoFactory = dsCfg.getFileIOFactory();

        marshaller = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());
    }

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        final GridKernalContext ctx = cctx.kernalContext();

        if (ctx.clientNode())
            return;

        final PdsFolderSettings folderSettings = ctx.pdsFolderResolver().resolveFolders();

        storeWorkDir = new File(folderSettings.persistentStoreRootPath(), folderSettings.folderName());

        U.ensureDirectory(storeWorkDir, "page store work directory", log);

        String tmpDir = System.getProperty("java.io.tmpdir");

        if (tmpDir != null && storeWorkDir.getAbsolutePath().startsWith(tmpDir)) {
            log.warning("Persistence store directory is in the temp directory and may be cleaned." +
                "To avoid this set \"IGNITE_HOME\" environment variable properly or " +
                "change location of persistence directories in data storage configuration " +
                "(see DataStorageConfiguration#walPath, DataStorageConfiguration#walArchivePath, " +
                "DataStorageConfiguration#storagePath properties). " +
                "Current persistence store directory is: [" + storeWorkDir.getAbsolutePath() + "]");
        }

        File[] files = storeWorkDir.listFiles();

        for (File file : files) {
            if (file.isDirectory()) {
                File[] tmpFiles = file.listFiles((k, v) -> v.endsWith(CACHE_DATA_TMP_FILENAME));

                if (tmpFiles != null) {
                    for (File tmpFile : tmpFiles) {
                        if (!tmpFile.delete())
                            log.warning("Failed to delete temporary cache config file" +
                                    "(make sure Ignite process has enough rights):" + file.getName());
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void cleanupPersistentSpace(CacheConfiguration cacheConfiguration) throws IgniteCheckedException {
        try {
            File cacheWorkDir = cacheWorkDir(cacheConfiguration);

            if (!cacheWorkDir.exists())
                return;

            try (DirectoryStream<Path> files = newDirectoryStream(cacheWorkDir.toPath(),
                new DirectoryStream.Filter<Path>() {
                    @Override public boolean accept(Path entry) throws IOException {
                        return entry.toFile().getName().endsWith(FILE_SUFFIX);
                    }
                })) {
                for (Path path : files)
                    delete(path);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to cleanup persistent directory: ", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cleanupPersistentSpace() throws IgniteCheckedException {
        try {
            try (DirectoryStream<Path> files = newDirectoryStream(
                storeWorkDir.toPath(), entry -> {
                    String name = entry.toFile().getName();

                    return !name.equals(META_STORAGE_NAME) &&
                        (name.startsWith(CACHE_DIR_PREFIX) || name.startsWith(CACHE_GRP_DIR_PREFIX));
                }
            )) {
                for (Path path : files)
                    U.delete(path);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to cleanup persistent directory: ", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cleanupPageStoreIfMatch(Predicate<Integer> cacheGrpPred, boolean cleanFiles) {
        Map<Integer, CacheStoreHolder> filteredStores = idxCacheStores.entrySet().stream()
            .filter(e -> cacheGrpPred.test(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        idxCacheStores.entrySet().removeIf(e -> cacheGrpPred.test(e.getKey()));

        Runnable doShutdown = () -> {
            IgniteCheckedException ex = shutdown(filteredStores.values(), cleanFiles);

            if (ex != null)
                U.error(log, "Failed to gracefully stop page store managers", ex);

            U.log(log, "Cleanup cache stores [total=" + filteredStores.keySet().size() +
                ", left=" + idxCacheStores.size() + ", cleanFiles=" + cleanFiles + ']');
        };

        if (cleanFiles) {
            cleanupAsyncExecutor.async(doShutdown);

            U.log(log, "Cache stores cleanup started asynchronously");
        }
        else
            doShutdown.run();
    }

    /** {@inheritDoc} */
    @Override public void stop0(boolean cancel) {
        if (log.isDebugEnabled())
            log.debug("Stopping page store manager.");

        cleanupPageStoreIfMatch(p -> true, false);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop0(boolean cancel) {
        cleanupAsyncExecutor.awaitAsyncTaskCompletion(cancel);
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activate page store manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        start0();
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (log.isDebugEnabled())
            log.debug("DeActivate page store manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        stop0(true);
    }

    /** {@inheritDoc} */
    @Override public void beginRecover() {
        for (CacheStoreHolder holder : idxCacheStores.values()) {
            holder.idxStore.beginRecover();

            for (PageStore partStore : holder.partStores)
                partStore.beginRecover();
        }
    }

    /** {@inheritDoc} */
    @Override public void finishRecover() throws IgniteCheckedException {
        try {
            for (CacheStoreHolder holder : idxCacheStores.values()) {
                holder.idxStore.finishRecover();

                for (PageStore partStore : holder.partStores)
                    partStore.finishRecover();
            }
        }
        catch (StorageException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void initialize(int cacheId, int partitions, String workingDir, LongAdderMetric tracker)
        throws IgniteCheckedException {
        assert storeWorkDir != null;

        if (!idxCacheStores.containsKey(cacheId)) {
            CacheStoreHolder holder = initDir(
                new File(storeWorkDir, workingDir),
                cacheId,
                partitions,
                tracker,
                cctx.cacheContext(cacheId) != null && cctx.cacheContext(cacheId).config().isEncryptionEnabled()
            );

            CacheStoreHolder old = idxCacheStores.put(cacheId, holder);

            assert old == null : "Non-null old store holder for cacheId: " + cacheId;
        }
    }

    /** {@inheritDoc} */
    @Override public void initializeForCache(CacheGroupDescriptor grpDesc, StoredCacheData cacheData) throws IgniteCheckedException {
        assert storeWorkDir != null;

        int grpId = grpDesc.groupId();

        if (!idxCacheStores.containsKey(grpId)) {
            CacheStoreHolder holder = initForCache(grpDesc, cacheData.config());

            CacheStoreHolder old = idxCacheStores.put(grpId, holder);

            assert old == null : "Non-null old store holder for cache: " + cacheData.config().getName();
        }
    }

    /** {@inheritDoc} */
    @Override public void initializeForMetastorage() throws IgniteCheckedException {
        assert storeWorkDir != null;

        int grpId = MetaStorage.METASTORAGE_CACHE_ID;

        if (!idxCacheStores.containsKey(grpId)) {
            DataRegion dataRegion = cctx.database().dataRegion(GridCacheDatabaseSharedManager.METASTORE_DATA_REGION_NAME);

            CacheStoreHolder holder = initDir(
                new File(storeWorkDir, META_STORAGE_NAME),
                grpId,
                PageIdAllocator.METASTORE_PARTITION + 1,
                dataRegion.memoryMetrics().totalAllocatedPages(),
                false);

            CacheStoreHolder old = idxCacheStores.put(grpId, holder);

            assert old == null : "Non-null old store holder for metastorage";
        }
    }

    /** {@inheritDoc} */
    @Override public void storeCacheData(StoredCacheData cacheData, boolean overwrite) throws IgniteCheckedException {
        CacheConfiguration<?, ?> ccfg = cacheData.config();
        File cacheWorkDir = cacheWorkDir(ccfg);

        checkAndInitCacheWorkDir(cacheWorkDir);

        assert cacheWorkDir.exists() : "Work directory does not exist: " + cacheWorkDir;

        File file = cacheConfigurationFile(ccfg);
        Path filePath = file.toPath();

        chgLock.readLock().lock();

        try {
            if (overwrite || !Files.exists(filePath) || Files.size(filePath) == 0) {
                File tmp = new File(file.getParent(), file.getName() + TMP_SUFFIX);

                if (tmp.exists() && !tmp.delete()) {
                    log.warning("Failed to delete temporary cache config file" +
                            "(make sure Ignite process has enough rights):" + file.getName());
                }

                // Pre-existing file will be truncated upon stream open.
                try (OutputStream stream = new BufferedOutputStream(new FileOutputStream(tmp))) {
                    marshaller.marshal(cacheData, stream);
                }

                if (Files.exists(filePath) && Files.size(filePath) > 0) {
                    for (BiConsumer<String, File> lsnr : lsnrs)
                        lsnr.accept(ccfg.getName(), file);
                }

                Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
        catch (IOException ex) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, ex));

            throw new IgniteCheckedException("Failed to persist cache configuration: " + ccfg.getName(), ex);
        }
        finally {
            chgLock.readLock().unlock();
        }
    }

    /**
     * @param lsnr Instance of listener to add.
     */
    public void addConfigurationChangeListener(BiConsumer<String, File> lsnr) {
        assert chgLock.isWriteLockedByCurrentThread();

        lsnrs.add(lsnr);
    }

    /**
     * @param lsnr Instance of listener to remove.
     */
    public void removeConfigurationChangeListener(BiConsumer<String, File> lsnr) {
        lsnrs.remove(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void shutdownForCacheGroup(CacheGroupContext grp, boolean destroy) throws IgniteCheckedException {
        grpsWithoutIdx.remove(grp.groupId());

        CacheStoreHolder old = idxCacheStores.remove(grp.groupId());

        if (old != null) {
            IgniteCheckedException ex = shutdown(old, /*clean files if destroy*/destroy, null);

            if (destroy)
                removeCacheGroupConfigurationData(grp);

            if (ex != null)
                throw ex;
        }
    }

    /** {@inheritDoc} */
    @Override public void onPartitionCreated(int grpId, int partId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onPartitionDestroyed(int grpId, int partId, int tag) throws IgniteCheckedException {
        assert partId <= MAX_PARTITION_ID;

        PageStore store = getStore(grpId, partId);

        store.truncate(tag);
    }

    /** {@inheritDoc} */
    @Override public void read(int grpId, long pageId, ByteBuffer pageBuf) throws IgniteCheckedException {
        read(grpId, pageId, pageBuf, false);
    }

    /**
     * Will preserve crc in buffer if keepCrc is true.
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     * @param pageBuf Page buffer.
     * @param keepCrc Keep CRC flag.
     * @throws IgniteCheckedException If failed.
     */
    public void read(int grpId, long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteCheckedException {
        PageStore store = getStore(grpId, PageIdUtils.partId(pageId));

        try {
            store.read(pageId, pageBuf, keepCrc);

            assert keepCrc || PageIO.getCrc(pageBuf) == 0 : store.size() - store.pageOffset(pageId);

            cctx.kernalContext().compress().decompressPage(pageBuf, store.getPageSize());
        }
        catch (StorageException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean exists(int grpId, int partId) throws IgniteCheckedException {
        PageStore store = getStore(grpId, partId);

        return store.exists();
    }

    /** {@inheritDoc} */
    @Override public void readHeader(int grpId, int partId, ByteBuffer buf) throws IgniteCheckedException {
        PageStore store = getStore(grpId, partId);

        try {
            store.readHeader(buf);
        }
        catch (StorageException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void write(int grpId, long pageId, ByteBuffer pageBuf, int tag) throws IgniteCheckedException {
        writeInternal(grpId, pageId, pageBuf, tag, true);
    }

    /** {@inheritDoc} */
    @Override public long pageOffset(int grpId, long pageId) throws IgniteCheckedException {
        PageStore store = getStore(grpId, PageIdUtils.partId(pageId));

        return store.pageOffset(pageId);
    }

    /**
     * @param cacheId Cache ID to write.
     * @param pageId Page ID.
     * @param pageBuf Page buffer.
     * @param tag Partition tag (growing 1-based partition file version). Used to validate page is not outdated
     * @param calculateCrc if {@code False} crc calculation will be forcibly skipped.
     * @return PageStore to which the page has been written.
     * @throws IgniteCheckedException If IO error occurred.
     */
    public PageStore writeInternal(int cacheId, long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc)
        throws IgniteCheckedException {
        int partId = PageIdUtils.partId(pageId);

        PageStore store = getStore(cacheId, partId);

        try {
            int pageSize = store.getPageSize();
            int compressedPageSize = pageSize;

            GridCacheContext cctx0 = cctx.cacheContext(cacheId);

            if (cctx0 != null) {
                assert pageBuf.position() == 0 && pageBuf.limit() == pageSize : pageBuf;

                ByteBuffer compressedPageBuf = cctx0.compress().compressPage(pageBuf, store);

                if (compressedPageBuf != pageBuf) {
                    compressedPageSize = PageIO.getCompressedSize(compressedPageBuf);

                    if (!calculateCrc) {
                        calculateCrc = true;
                        PageIO.setCrc(compressedPageBuf, 0); // It will be recalculated over compressed data further.
                    }

                    PageIO.setCrc(pageBuf, 0); // It is expected to be reset to 0 after each write.
                    pageBuf = compressedPageBuf;
                }
            }

            store.write(pageId, pageBuf, tag, calculateCrc);

            if (pageSize > compressedPageSize)
                store.punchHole(pageId, compressedPageSize); // TODO maybe add async punch mode?
        }
        catch (StorageException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }

        return store;
    }

    /**
     *
     */
    public Path getPath(boolean isSharedGroup, String cacheOrGroupName, int partId) {
        return getPartitionFilePath(cacheWorkDir(isSharedGroup, cacheOrGroupName), partId);
    }

    /**
     * @param grpDesc Cache group descriptor.
     * @param ccfg Cache configuration.
     * @return Cache store holder.
     * @throws IgniteCheckedException If failed.
     */
    private CacheStoreHolder initForCache(CacheGroupDescriptor grpDesc, CacheConfiguration ccfg) throws IgniteCheckedException {
        assert !grpDesc.sharedGroup() || ccfg.getGroupName() != null : ccfg.getName();

        File cacheWorkDir = cacheWorkDir(ccfg);

        String dataRegionName = grpDesc.config().getDataRegionName();

        DataRegionMetricsImpl regionMetrics = cctx.database().dataRegion(dataRegionName).memoryMetrics();

        LongAdderMetric allocatedTracker =
            regionMetrics.getOrAllocateGroupPageAllocationTracker(grpDesc.cacheOrGroupName());

        return initDir(
            cacheWorkDir,
            grpDesc.groupId(),
            grpDesc.config().getAffinity().partitions(),
            allocatedTracker,
            ccfg.isEncryptionEnabled()
        );
    }

    /**
     * @param grpId Cache group id.
     * @param encrypted {@code true} if cache group encryption enabled.
     * @return Factory to create page stores.
     */
    public FilePageStoreFactory getPageStoreFactory(int grpId, boolean encrypted) {
        FileIOFactory pageStoreFileIoFactory = this.pageStoreFileIoFactory;
        FileIOFactory pageStoreV1FileIoFactory = this.pageStoreV1FileIoFactory;

        if (encrypted) {
            pageStoreFileIoFactory = new EncryptedFileIOFactory(
                this.pageStoreFileIoFactory,
                grpId,
                pageSize(),
                cctx.kernalContext().encryption(),
                cctx.gridConfig().getEncryptionSpi());

            pageStoreV1FileIoFactory = new EncryptedFileIOFactory(
                this.pageStoreV1FileIoFactory,
                grpId,
                pageSize(),
                cctx.kernalContext().encryption(),
                cctx.gridConfig().getEncryptionSpi());
        }

        FileVersionCheckingFactory pageStoreFactory = new FileVersionCheckingFactory(
            pageStoreFileIoFactory,
            pageStoreV1FileIoFactory,
            igniteCfg.getDataStorageConfiguration()
        );

        if (encrypted) {
            int headerSize = pageStoreFactory.headerSize(pageStoreFactory.latestVersion());

            ((EncryptedFileIOFactory)pageStoreFileIoFactory).headerSize(headerSize);
            ((EncryptedFileIOFactory)pageStoreV1FileIoFactory).headerSize(headerSize);
        }

        return pageStoreFactory;
    }

    /**
     * @param cacheWorkDir Work directory.
     * @param grpId Group ID.
     * @param partitions Number of partitions.
     * @param allocatedTracker Metrics updater.
     * @param encrypted {@code True} if this cache encrypted.
     * @return Cache store holder.
     * @throws IgniteCheckedException If failed.
     */
    private CacheStoreHolder initDir(File cacheWorkDir,
        int grpId,
        int partitions,
        LongAdderMetric allocatedTracker,
        boolean encrypted) throws IgniteCheckedException {
        try {
            boolean dirExisted = checkAndInitCacheWorkDir(cacheWorkDir);

            File idxFile = new File(cacheWorkDir, INDEX_FILE_NAME);

            if (dirExisted && !idxFile.exists())
                grpsWithoutIdx.add(grpId);

            FilePageStoreFactory pageStoreFactory = getPageStoreFactory(grpId, encrypted);

            PageStore idxStore =
                pageStoreFactory.createPageStore(
                    PageMemory.FLAG_IDX,
                    idxFile,
                    allocatedTracker);

            PageStore[] partStores = new PageStore[partitions];

            for (int partId = 0; partId < partStores.length; partId++) {
                final int p = partId;

                PageStore partStore =
                    pageStoreFactory.createPageStore(
                        PageMemory.FLAG_DATA,
                        () -> getPartitionFilePath(cacheWorkDir, p),
                        allocatedTracker);

                    partStores[partId] = partStore;
                }

            return new CacheStoreHolder(idxStore, partStores);
        }
        catch (IgniteCheckedException e) {
            if (X.hasCause(e, StorageException.class, IOException.class))
                cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /**
     * @param cacheWorkDir Cache work directory.
     * @param partId Partition id.
     */
    @NotNull private Path getPartitionFilePath(File cacheWorkDir, int partId) {
        return new File(cacheWorkDir, getPartitionFileName(partId)).toPath();
    }

    /**
     * @param workDir Cache work directory.
     * @param cacheDirName Cache directory name.
     * @param partId Partition id.
     * @return Partition file.
     */
    @NotNull public static File getPartitionFile(File workDir, String cacheDirName, int partId) {
        return new File(cacheWorkDir(workDir, cacheDirName), getPartitionFileName(partId));
    }

    /**
     * @param partId Partition id.
     * @return File name.
     */
    public static String getPartitionFileName(int partId) {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;

        return partId == INDEX_PARTITION ? INDEX_FILE_NAME : format(PART_FILE_TEMPLATE, partId);
    }

    /** {@inheritDoc} */
    @Override public boolean checkAndInitCacheWorkDir(CacheConfiguration cacheCfg) throws IgniteCheckedException {
        return checkAndInitCacheWorkDir(cacheWorkDir(cacheCfg));
    }

    /**
     * @param cacheWorkDir Cache work directory.
     */
    private boolean checkAndInitCacheWorkDir(File cacheWorkDir) throws IgniteCheckedException {
        boolean dirExisted = false;

        ReadWriteLock lock = initDirLock.getLock(cacheWorkDir.getName().hashCode());

        lock.writeLock().lock();

        try {
            if (!Files.exists(cacheWorkDir.toPath())) {
                try {
                    Files.createDirectory(cacheWorkDir.toPath());
                }
                catch (IOException e) {
                    throw new IgniteCheckedException("Failed to initialize cache working directory " +
                        "(failed to create, make sure the work folder has correct permissions): " +
                        cacheWorkDir.getAbsolutePath(), e);
                }
            }
            else {
                if (cacheWorkDir.isFile())
                    throw new IgniteCheckedException("Failed to initialize cache working directory " +
                        "(a file with the same name already exists): " + cacheWorkDir.getAbsolutePath());

                File lockF = new File(cacheWorkDir, IgniteCacheSnapshotManager.SNAPSHOT_RESTORE_STARTED_LOCK_FILENAME);

                Path cacheWorkDirPath = cacheWorkDir.toPath();

                Path tmp = cacheWorkDirPath.getParent().resolve(cacheWorkDir.getName() + TMP_SUFFIX);

                if (Files.exists(tmp) && Files.isDirectory(tmp) &&
                    Files.exists(tmp.resolve(IgniteCacheSnapshotManager.TEMP_FILES_COMPLETENESS_MARKER))) {

                    U.warn(log, "Ignite node crashed during the snapshot restore process " +
                        "(there is a snapshot restore lock file left for cache). But old version of cache was saved. " +
                        "Trying to restore it. Cache - [" + cacheWorkDir.getAbsolutePath() + ']');

                    U.delete(cacheWorkDir);

                    try {
                        Files.move(tmp, cacheWorkDirPath, StandardCopyOption.ATOMIC_MOVE);

                        cacheWorkDirPath.resolve(IgniteCacheSnapshotManager.TEMP_FILES_COMPLETENESS_MARKER).toFile().delete();
                    }
                    catch (IOException e) {
                        throw new IgniteCheckedException(e);
                    }
                }
                else if (lockF.exists()) {
                    U.warn(log, "Ignite node crashed during the snapshot restore process " +
                        "(there is a snapshot restore lock file left for cache). Will remove both the lock file and " +
                        "incomplete cache directory [cacheDir=" + cacheWorkDir.getAbsolutePath() + ']');

                    boolean deleted = U.delete(cacheWorkDir);

                    if (!deleted)
                        throw new IgniteCheckedException("Failed to remove obsolete cache working directory " +
                            "(remove the directory manually and make sure the work folder has correct permissions): " +
                            cacheWorkDir.getAbsolutePath());

                    cacheWorkDir.mkdirs();
                }
                else
                    dirExisted = true;

                if (!cacheWorkDir.exists())
                    throw new IgniteCheckedException("Failed to initialize cache working directory " +
                        "(failed to create, make sure the work folder has correct permissions): " +
                        cacheWorkDir.getAbsolutePath());

                if (Files.exists(tmp))
                    U.delete(tmp);
            }
        }
        finally {
            lock.writeLock().unlock();
        }

        return dirExisted;
    }

    /** {@inheritDoc} */
    @Override public void sync(int grpId, int partId) throws IgniteCheckedException {
        try {
            getStore(grpId, partId).sync();
        }
        catch (StorageException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void ensure(int grpId, int partId) throws IgniteCheckedException {
        try {
            getStore(grpId, partId).ensure();
        }
        catch (StorageException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public long allocatePage(int grpId, int partId, byte flags) throws IgniteCheckedException {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;

        PageStore store = getStore(grpId, partId);

        try {
            long pageIdx = store.allocatePage();

            return PageIdUtils.pageId(partId, flags, (int)pageIdx);
        }
        catch (StorageException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public long metaPageId(final int grpId) {
        return metaPageId;
    }

    /** {@inheritDoc} */
    @Override public int pages(int grpId, int partId) throws IgniteCheckedException {
        PageStore store = getStore(grpId, partId);

        return store.pages();
    }

    /**
     * @param ccfgs List of cache configurations to process.
     * @param ccfgCons Consumer which accepts found configurations files.
     */
    public void readConfigurationFiles(List<CacheConfiguration<?, ?>> ccfgs,
        BiConsumer<CacheConfiguration<?, ?>, File> ccfgCons) {
        chgLock.writeLock().lock();

        try {
            for (CacheConfiguration<?, ?> ccfg : ccfgs) {
                File cacheDir = cacheWorkDir(ccfg);

                if (!cacheDir.exists())
                    continue;

                File[] ccfgFiles = cacheDir.listFiles((dir, name) ->
                    name.endsWith(CACHE_DATA_FILENAME));

                if (ccfgFiles == null)
                    continue;

                for (File ccfgFile : ccfgFiles)
                    ccfgCons.accept(ccfg, ccfgFile);
            }
        }
        finally {
            chgLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<String, StoredCacheData> readCacheConfigurations() throws IgniteCheckedException {
        if (cctx.kernalContext().clientNode())
            return Collections.emptyMap();

        File[] files = storeWorkDir.listFiles();

        if (files == null)
            return Collections.emptyMap();

        Map<String, StoredCacheData> ccfgs = new HashMap<>();

        Arrays.sort(files);

        for (File file : files) {
            if (file.isDirectory()) {
                if (file.getName().startsWith(CACHE_DIR_PREFIX)) {
                    File conf = new File(file, CACHE_DATA_FILENAME);

                    if (conf.exists() && conf.length() > 0) {
                        StoredCacheData cacheData = readCacheData(conf);

                        String cacheName = cacheData.config().getName();

                        if (!ccfgs.containsKey(cacheName))
                            ccfgs.put(cacheName, cacheData);
                        else {
                            U.warn(log, "Cache with name=" + cacheName + " is already registered, skipping config file "
                                    + file.getName());
                        }
                    }
                }
                else if (file.getName().startsWith(CACHE_GRP_DIR_PREFIX))
                    readCacheGroupCaches(file, ccfgs);
            }
        }

        return ccfgs;
    }

    /**
     * @param grpDir Group directory.
     * @param ccfgs Cache configurations.
     * @throws IgniteCheckedException If failed.
     */
    private void readCacheGroupCaches(File grpDir, Map<String, StoredCacheData> ccfgs) throws IgniteCheckedException {
        File[] files = grpDir.listFiles();

        if (files == null)
            return;

        for (File file : files) {
            if (!file.isDirectory() && file.getName().endsWith(CACHE_DATA_FILENAME) && file.length() > 0) {
                StoredCacheData cacheData = readCacheData(file);

                String cacheName = cacheData.config().getName();

                if (!ccfgs.containsKey(cacheName))
                    ccfgs.put(cacheName, cacheData);
                else {
                    U.warn(log, "Cache with name=" + cacheName + " is already registered, skipping config file "
                            + file.getName() + " in group directory " + grpDir.getName());
                }
            }
        }
    }

    /**
     * @param conf File with stored cache data.
     * @return Cache data.
     * @throws IgniteCheckedException If failed.
     */
    private StoredCacheData readCacheData(File conf) throws IgniteCheckedException {
        try (InputStream stream = new BufferedInputStream(new FileInputStream(conf))) {
            return marshaller.unmarshal(stream, U.resolveClassLoader(igniteCfg));
        }
        catch (IgniteCheckedException | IOException e) {
                throw new IgniteCheckedException("An error occurred during cache configuration loading from file [file=" +
                    conf.getAbsolutePath() + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasIndexStore(int grpId) {
        return !grpsWithoutIdx.contains(grpId);
    }

    /** {@inheritDoc} */
    @Override public long pagesAllocated(int grpId) {
        CacheStoreHolder holder = idxCacheStores.get(grpId);

        if (holder == null)
            return 0;

        long pageCnt = holder.idxStore.pages();

        for (int i = 0; i < holder.partStores.length; i++)
            pageCnt += holder.partStores[i].pages();

        return pageCnt;
    }

    /**
     * @return Store work dir. Includes consistent-id based folder
     */
    public File workDir() {
        return storeWorkDir;
    }

    /**
     * @param ccfg Cache configuration.
     * @return Store dir for given cache.
     */
    public File cacheWorkDir(CacheConfiguration<?, ?> ccfg) {
        return cacheWorkDir(storeWorkDir, cacheDirName(ccfg));
    }

    /**
     * @param isSharedGroup {@code True} if cache is sharing the same `underlying` cache.
     * @param cacheOrGroupName Cache name.
     * @return Store directory for given cache.
     */
    public File cacheWorkDir(boolean isSharedGroup, String cacheOrGroupName) {
        return cacheWorkDir(storeWorkDir, cacheDirName(isSharedGroup, cacheOrGroupName));
    }

    /**
     * @param cacheDirName Cache directory name.
     * @return Store directory for given cache.
     */
    public static File cacheWorkDir(File storeWorkDir, String cacheDirName) {
        return new File(storeWorkDir, cacheDirName);
    }

    /**
     * @param isSharedGroup {@code True} if cache is sharing the same `underlying` cache.
     * @param cacheOrGroupName Cache name.
     * @return The full cache directory name.
     */
    public static String cacheDirName(boolean isSharedGroup, String cacheOrGroupName) {
        return isSharedGroup ? CACHE_GRP_DIR_PREFIX + cacheOrGroupName
            : CACHE_DIR_PREFIX + cacheOrGroupName;
    }

    /**
     * @param ccfg Cache configuration.
     * @return The full cache directory name.
     */
    public static String cacheDirName(CacheConfiguration<?, ?> ccfg) {
        boolean isSharedGrp = ccfg.getGroupName() != null;

        return cacheDirName(isSharedGrp, isSharedGrp ? ccfg.getGroupName() : ccfg.getName());
    }

    /**
     * @param cleanFiles {@code True} if the stores should delete it's files upon close.
     */
    private IgniteCheckedException shutdown(Collection<CacheStoreHolder> holders, boolean cleanFiles) {
        IgniteCheckedException ex = null;

        for (CacheStoreHolder holder : holders)
            ex = shutdown(holder, cleanFiles, ex);

        return ex;
    }

    /**
     * @param holder Store holder.
     * @param cleanFile {@code True} if files should be cleaned.
     * @param aggr Aggregating exception.
     * @return Aggregating exception, if error occurred.
     */
    private IgniteCheckedException shutdown(CacheStoreHolder holder, boolean cleanFile,
        @Nullable IgniteCheckedException aggr) {
        aggr = shutdown(holder.idxStore, cleanFile, aggr);

        for (PageStore store : holder.partStores) {
            if (store != null)
                aggr = shutdown(store, cleanFile, aggr);
        }

        return aggr;
    }

    /**
     * Delete caches' configuration data files of cache group.
     *
     * @param ctx Cache group context.
     * @throws IgniteCheckedException If fails.
     */
    private void removeCacheGroupConfigurationData(CacheGroupContext ctx) throws IgniteCheckedException {
        File cacheGrpDir = cacheWorkDir(ctx.sharedGroup(), ctx.cacheOrGroupName());

        if (cacheGrpDir != null && cacheGrpDir.exists()) {
            DirectoryStream.Filter<Path> cacheCfgFileFilter = new DirectoryStream.Filter<Path>() {
                @Override public boolean accept(Path path) {
                    return Files.isRegularFile(path) && path.getFileName().toString().endsWith(CACHE_DATA_FILENAME);
                }
            };

            try (DirectoryStream<Path> dirStream = newDirectoryStream(cacheGrpDir.toPath(), cacheCfgFileFilter)) {
                for (Path path: dirStream)
                    Files.deleteIfExists(path);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to delete cache configurations of group: " + ctx.toString(), e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void removeCacheData(StoredCacheData cacheData) throws IgniteCheckedException {
        chgLock.readLock().lock();

        try {
            CacheConfiguration<?, ?> ccfg = cacheData.config();
            File file = cacheConfigurationFile(ccfg);

            if (file.exists()) {
                for (BiConsumer<String, File> lsnr : lsnrs)
                    lsnr.accept(ccfg.getName(), file);

                if (!file.delete())
                    throw new IgniteCheckedException("Failed to delete cache configuration: " + ccfg.getName());
            }
        }
        finally {
            chgLock.readLock().unlock();
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @return Cache configuration file with respect to {@link CacheConfiguration#getGroupName} value.
     */
    private File cacheConfigurationFile(CacheConfiguration<?, ?> ccfg) {
        File cacheWorkDir = cacheWorkDir(ccfg);

        return ccfg.getGroupName() == null ? new File(cacheWorkDir, CACHE_DATA_FILENAME) :
            new File(cacheWorkDir, ccfg.getName() + CACHE_DATA_FILENAME);
    }

    /**
     * @param store Store to shutdown.
     * @param cleanFile {@code True} if files should be cleaned.
     * @param aggr Aggregating exception.
     * @return Aggregating exception, if error occurred.
     */
    private IgniteCheckedException shutdown(PageStore store, boolean cleanFile, IgniteCheckedException aggr) {
        try {
            if (store != null)
                store.stop(cleanFile);
        }
        catch (IgniteCheckedException e) {
            if (aggr == null)
                aggr = new IgniteCheckedException("Failed to gracefully shutdown store");

            aggr.addSuppressed(e);
        }

        return aggr;
    }

    /**
     * Return cache store holedr.
     *
     * @param grpId Cache group ID.
     * @return Cache store holder.
     */
    private CacheStoreHolder getHolder(int grpId) throws IgniteCheckedException {
        try {
            return idxCacheStores.computeIfAbsent(grpId, (key) -> {
                CacheGroupDescriptor gDesc = cctx.cache().cacheGroupDescriptor(grpId);

                CacheStoreHolder holder0 = null;

                if (gDesc != null && CU.isPersistentCache(gDesc.config(), cctx.gridConfig().getDataStorageConfiguration())) {
                    try {
                        holder0 = initForCache(gDesc, gDesc.config());
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }

                return holder0;
            });
        } catch (IgniteException ex) {
            if (X.hasCause(ex, IgniteCheckedException.class))
                throw ex.getCause(IgniteCheckedException.class);
            else
                throw ex;
        }
    }

    /**
     * @param grpId Cache group ID.
     * @return Collection of related page stores.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<PageStore> getStores(int grpId) throws IgniteCheckedException {
        return getHolder(grpId);
    }

    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return Page store for the corresponding parameters.
     * @throws IgniteCheckedException If cache or partition with the given ID was not created.
     *
     * Note: visible for testing.
     */
    public PageStore getStore(int grpId, int partId) throws IgniteCheckedException {
        CacheStoreHolder holder = getHolder(grpId);

        if (holder == null)
            throw new IgniteCheckedException("Failed to get page store for the given cache ID " +
                "(cache has not been started): " + grpId);

        if (partId == INDEX_PARTITION)
            return holder.idxStore;

        if (partId > MAX_PARTITION_ID)
            throw new IgniteCheckedException("Partition ID is reserved: " + partId);

        PageStore store = holder.partStores[partId];

        if (store == null)
            throw new IgniteCheckedException("Failed to get page store for the given partition ID " +
                "(partition has not been created) [grpId=" + grpId + ", partId=" + partId + ']');

        return store;
    }

    /** {@inheritDoc} */
    @Override public void beforeCacheGroupStart(CacheGroupDescriptor grpDesc) {
        if (grpDesc.persistenceEnabled()) {
            boolean localEnabled = cctx.database().walEnabled(grpDesc.groupId(), true);
            boolean globalEnabled = cctx.database().walEnabled(grpDesc.groupId(), false);

            if (!localEnabled || !globalEnabled) {
                File dir = cacheWorkDir(grpDesc.config());

                assert dir.exists();

                boolean res = IgniteUtils.delete(dir);

                assert res;

                if (!globalEnabled)
                    grpDesc.walEnabled(false);
            }
        }
    }

    /**
     * @param pageStoreFileIoFactory File IO factory to override default, may be used for blocked read-write.
     * @param pageStoreV1FileIoFactory File IO factory for reading V1 page store and for fast touching page files
     *      (non blocking).
     */
    public void setPageStoreFileIOFactories(final FileIOFactory pageStoreFileIoFactory,
        final FileIOFactory pageStoreV1FileIoFactory) {
        this.pageStoreFileIoFactory = pageStoreFileIoFactory;
        this.pageStoreV1FileIoFactory = pageStoreV1FileIoFactory;
    }

    /**
     * @return File IO factory currently selected for page store.
     */
    public FileIOFactory getPageStoreFileIoFactory() {
        return pageStoreFileIoFactory;
    }

    /**
     * @return Durable memory page size in bytes.
     */
    public int pageSize() {
        return dsCfg.getPageSize();
    }

    /**
     *
     */
    private static class CacheStoreHolder extends AbstractList<PageStore> {
        /** Index store. */
        private final PageStore idxStore;

        /** Partition stores. */
        private final PageStore[] partStores;

        /**
         */
        CacheStoreHolder(PageStore idxStore, PageStore[] partStores) {
            this.idxStore = requireNonNull(idxStore);
            this.partStores = requireNonNull(partStores);
        }

        /** {@inheritDoc} */
        @Override public PageStore get(int idx) {
            return requireNonNull(idx == partStores.length ? idxStore : partStores[idx]);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return partStores.length + 1;
        }
    }

    /**
     * Synchronization wrapper for long operations that should be executed asynchronously
     * and operations that can not be executed in parallel with long operation. Uses {@link ReadWriteLock}
     * to provide such synchronization scenario.
     */
    protected static class LongOperationAsyncExecutor {
        /** */
        private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

        /** */
        private final String igniteInstanceName;

        /** */
        private final IgniteLogger log;

        /** */
        private Set<GridWorker> workers = new GridConcurrentHashSet<>();

        /** */
        private static final AtomicLong workerCounter = new AtomicLong(0);

        /** */
        public LongOperationAsyncExecutor(String igniteInstanceName, IgniteLogger log) {
            this.igniteInstanceName = igniteInstanceName;

            this.log = log;
        }

        /**
         * Executes long operation in dedicated thread. Uses write lock as such operations can't run
         * simultaneously.
         *
         * @param runnable long operation
         */
        public void async(Runnable runnable) {
            String workerName = "async-file-store-cleanup-task-" + workerCounter.getAndIncrement();

            GridWorker worker = new GridWorker(igniteInstanceName, workerName, log) {
                @Override protected void body() {
                    readWriteLock.writeLock().lock();

                    try {
                        runnable.run();
                    }
                    finally {
                        readWriteLock.writeLock().unlock();

                        workers.remove(this);
                    }
                }
            };

            workers.add(worker);

            Thread asyncTask = new IgniteThread(worker);

            asyncTask.start();
        }

        /**
         * Executes closure that can't run in parallel with long operation that is executed by
         * {@link LongOperationAsyncExecutor#async}. Uses read lock as such closures can run in parallel with
         * each other.
         *
         * @param closure closure.
         * @param <T> return type.
         * @return value that is returned by {@code closure}.
         */
        public <T> T afterAsyncCompletion(IgniteOutClosure<T> closure) {
            readWriteLock.readLock().lock();
            try {
                return closure.apply();
            }
            finally {
                readWriteLock.readLock().unlock();
            }
        }

        /**
         * Cancels async tasks.
         */
        public void awaitAsyncTaskCompletion(boolean cancel) {
            U.awaitForWorkersStop(workers, cancel, log);
        }
    }

    /**
     * Proxy class for {@link FilePageStoreManager#idxCacheStores} map that wraps data adding and replacing
     * operations to disallow concurrent execution simultaneously with cleanup of file page storage. Wrapping
     * of data removing operations is not needed.
     *
     * @param <K> key type
     * @param <V> value type
     */
    private static class IdxCacheStores<K, V> extends ConcurrentHashMap<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Executor that wraps data adding and replacing operations.
         */
        private final LongOperationAsyncExecutor longOperationAsyncExecutor;

        /**
         * Default constructor.
         *
         * @param longOperationAsyncExecutor executor that wraps data adding and replacing operations.
         */
        IdxCacheStores(LongOperationAsyncExecutor longOperationAsyncExecutor) {
            super();

            this.longOperationAsyncExecutor = longOperationAsyncExecutor;
        }

        /** {@inheritDoc} */
        @Override public V put(K key, V val) {
            return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.put(key, val));
        }

        /** {@inheritDoc} */
        @Override public void putAll(Map<? extends K, ? extends V> m) {
            longOperationAsyncExecutor.afterAsyncCompletion(() -> {
                super.putAll(m);

                return null;
            });
        }

        /** {@inheritDoc} */
        @Override public V putIfAbsent(K key, V val) {
            return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.putIfAbsent(key, val));
        }

        /** {@inheritDoc} */
        @Override public boolean replace(K key, V oldVal, V newVal) {
            return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.replace(key, oldVal, newVal));
        }

        /** {@inheritDoc} */
        @Override public V replace(K key, V val) {
            return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.replace(key, val));
        }

        /** {@inheritDoc} */
        @Override public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
            return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.computeIfAbsent(key, mappingFunction));
        }

        /** {@inheritDoc} */
        @Override public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
            return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.computeIfPresent(key, remappingFunction));
        }

        /** {@inheritDoc} */
        @Override public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
            return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.compute(key, remappingFunction));
        }

        /** {@inheritDoc} */
        @Override public V merge(K key, V val, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
            return longOperationAsyncExecutor.afterAsyncCompletion(() -> super.merge(key, val, remappingFunction));
        }
    }
}
