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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
import org.apache.ignite.internal.managers.encryption.EncryptionCacheKeyProvider;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.store.PageStoreCollection;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageReadWriteManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageReadWriteManagerImpl;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridStripedReadWriteLock;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static java.nio.file.Files.delete;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.CACHE_DIR_WITH_META_FILTER;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.PART_FILE_PREFIX;

/**
 * File page store manager.
 */
public class FilePageStoreManager extends GridCacheSharedManagerAdapter implements IgnitePageStoreManager,
    PageStoreCollection {
    /** File suffix. */
    public static final String FILE_SUFFIX = ".bin";

    /** Suffix for zip files */
    public static final String ZIP_SUFFIX = ".zip";

    /** Suffix for tmp files */
    public static final String TMP_SUFFIX = ".tmp";

    /** */
    public static final String INDEX_FILE_PREFIX = "index";

    /** */
    public static final String INDEX_FILE_NAME = INDEX_FILE_PREFIX + FILE_SUFFIX;

    /** */
    public static final String PART_FILE_TEMPLATE = PART_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** */
    public static final String CACHE_DATA_FILENAME = "cache_data.dat";

    /** */
    public static final String CACHE_DATA_TMP_FILENAME = CACHE_DATA_FILENAME + TMP_SUFFIX;

    /** */
    public static final String DFLT_STORE_DIR = "db";

    /** Matcher for searching of *.tmp files. */
    public static final PathMatcher TMP_FILE_MATCHER =
        FileSystems.getDefault().getPathMatcher("glob:**" + TMP_SUFFIX);

    /** Unique name for corrupted data files maintenance task. */
    public static final String CORRUPTED_DATA_FILES_MNTC_TASK_NAME = "corrupted-cache-data-files-task";

    /** Page manager. */
    private final PageReadWriteManager pmPageMgr;

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
     * May be overridden by block read/write.
     */
    private FileIOFactory pageStoreFileIoFactory;

    /**
     * File IO factory for page store V1 and for fast checking page store (non block read).
     * By default is taken from {@link #dsCfg}.
     */
    private FileIOFactory pageStoreV1FileIoFactory;

    /** */
    private final DataStorageConfiguration dsCfg;

    /** Node file tree. */
    private NodeFileTree ft;

    /** */
    private final Set<Integer> grpsWithoutIdx = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    /** */
    private static final GridStripedReadWriteLock initDirLock =
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

        pmPageMgr = new PageReadWriteManagerImpl(ctx, this, FilePageStoreManager.class.getSimpleName());
    }

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        final GridKernalContext ctx = cctx.kernalContext();

        if (ctx.clientNode())
            return;

        ft = ctx.pdsFolderResolver().fileTree();

        U.ensureDirectory(ft.nodeStorage(), "page store work directory", log);

        String tmpDir = System.getProperty("java.io.tmpdir");

        if (tmpDir != null && ft.nodeStorage().getAbsolutePath().startsWith(tmpDir)) {
            log.warning("Persistence store directory is in the temp directory and may be cleaned." +
                "To avoid this set \"IGNITE_HOME\" environment variable properly or " +
                "change location of persistence directories in data storage configuration " +
                "(see DataStorageConfiguration#walPath, DataStorageConfiguration#walArchivePath, " +
                "DataStorageConfiguration#storagePath properties). " +
                "Current persistence store directory is: [" + ft.nodeStorage().getAbsolutePath() + "]");
        }

        File[] files = ft.nodeStorage().listFiles();

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
            File cacheWorkDir = ft.cacheStorage(cacheConfiguration);

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
                ft.nodeStorage().toPath(), entry -> NodeFileTree.CACHE_DIR_FILTER.test(entry.toFile())
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
        List<CacheConfiguration> cacheCfgs = findCacheGroupsWithDisabledWal();

        if (!cacheCfgs.isEmpty()) {
            List<String> cacheGrpNames = cacheCfgs.stream()
                .map(ccfg -> ccfg.getGroupName() != null ? ccfg.getGroupName() : ccfg.getName())
                .collect(Collectors.toList());

            String errorMsg = "Ignite node with disabled WAL was stopped in the middle of a checkpoint, " +
                "data files may be corrupted. Node will stop and enter the Maintenance Mode on next start. " +
                "In the Maintenance Mode, use the Control Utility *persistence* command " +
                "to clean and optionally back up corrupted files. When cleaning is done, restart the node manually. " +
                "Possible corruption affects the following cache groups: " + cacheGrpNames;

            log.warning(errorMsg);

            try {
                cctx.kernalContext().maintenanceRegistry()
                    .registerMaintenanceTask(
                        new MaintenanceTask(CORRUPTED_DATA_FILES_MNTC_TASK_NAME,
                            "Corrupted cache groups found",
                            cacheCfgs.stream()
                                .map(ccfg -> ft.cacheStorage(ccfg).getName())
                                .collect(Collectors.joining(File.separator)))
                );
            }
            catch (IgniteCheckedException e) {
                log.warning("Failed to register maintenance record for corrupted partition files.", e);
            }

            throw new IgniteException(errorMsg);
        }

        for (CacheStoreHolder holder : idxCacheStores.values()) {
            holder.idxStore.beginRecover();

            for (PageStore partStore : holder.partStores)
                partStore.beginRecover();
        }
    }

    /**
     * Checks cache groups' settings and returns configurations of cache groups with disabled WAL.
     *
     * @return List of cache groups' configurations that had WAL disabled before node stop.
     */
    private List<CacheConfiguration> findCacheGroupsWithDisabledWal() {
        List<CacheConfiguration> corruptedCacheGrps = new ArrayList<>();

        for (Integer grpDescId : idxCacheStores.keySet()) {
            CacheGroupDescriptor desc = cctx.cache().cacheGroupDescriptor(grpDescId);

            if (desc != null && desc.persistenceEnabled()) {
                boolean locEnabled = cctx.database().walEnabled(grpDescId, true);
                boolean globalEnabled = cctx.database().walEnabled(grpDescId, false);

                if (!locEnabled || !globalEnabled) {
                    File dir = ft.cacheStorage(desc.config());

                    if (Arrays.stream(
                        dir.listFiles()).anyMatch(f -> !f.getName().equals(CACHE_DATA_FILENAME))) {
                        corruptedCacheGrps.add(desc.config());
                    }
                }
            }
        }

        return corruptedCacheGrps;
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
    @Override public void initializeForCache(CacheGroupDescriptor grpDesc, CacheConfiguration<?, ?> ccfg) throws IgniteCheckedException {
        assert ft != null;

        int grpId = grpDesc.groupId();

        if (!idxCacheStores.containsKey(grpId)) {
            CacheStoreHolder holder = initForCache(grpDesc, ccfg);

            CacheStoreHolder old = idxCacheStores.put(grpId, holder);

            assert old == null : "Non-null old store holder for cache: " + ccfg.getName();
        }
    }

    /** {@inheritDoc} */
    @Override public void initializeForMetastorage() throws IgniteCheckedException {
        assert ft.nodeStorage() != null;

        int grpId = MetaStorage.METASTORAGE_CACHE_ID;

        if (!idxCacheStores.containsKey(grpId)) {
            DataRegion dataRegion = cctx.database().dataRegion(GridCacheDatabaseSharedManager.METASTORE_DATA_REGION_NAME);
            PageMetrics pageMetrics = dataRegion.metrics().cacheGrpPageMetrics(grpId);

            CacheStoreHolder holder = initDir(
                new File(ft.nodeStorage(), MetaStorage.METASTORAGE_DIR_NAME),
                grpId,
                MetaStorage.METASTORAGE_CACHE_NAME,
                MetaStorage.METASTORAGE_PARTITIONS.size(),
                pageMetrics,
                false,
                null);

            CacheStoreHolder old = idxCacheStores.put(grpId, holder);

            assert old == null : "Non-null old store holder for metastorage";
        }
    }

    /** {@inheritDoc} */
    @Override public void shutdownForCacheGroup(CacheGroupContext grp, boolean destroy) throws IgniteCheckedException {
        grpsWithoutIdx.remove(grp.groupId());

        CacheStoreHolder old = idxCacheStores.remove(grp.groupId());

        if (old != null) {
            IgniteCheckedException ex = shutdown(old, /*clean files if destroy*/destroy, null);

            if (ex != null)
                throw ex;
        }
    }

    /** {@inheritDoc} */
    @Override public void truncate(int grpId, int partId, int tag) throws IgniteCheckedException {
        assert partId <= MAX_PARTITION_ID;

        PageStore store = getStore(grpId, partId);

        store.truncate(tag);
    }

    /** {@inheritDoc} */
    @Override public void read(int grpId, long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteCheckedException {
        pmPageMgr.read(grpId, pageId, pageBuf, keepCrc);
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
    @Override public PageStore write(
        int grpId,
        long pageId,
        ByteBuffer pageBuf,
        int tag,
        boolean calculateCrc
    ) throws IgniteCheckedException {
        return pmPageMgr.write(grpId, pageId, pageBuf, tag, calculateCrc);
    }

    /** {@inheritDoc} */
    @Override public long pageOffset(int grpId, long pageId) throws IgniteCheckedException {
        PageStore store = getStore(grpId, PageIdUtils.partId(pageId));

        return store.pageOffset(pageId);
    }

    /**
     * @param grpDesc Cache group descriptor.
     * @param ccfg Cache configuration.
     * @return Cache store holder.
     * @throws IgniteCheckedException If failed.
     */
    private CacheStoreHolder initForCache(CacheGroupDescriptor grpDesc, CacheConfiguration ccfg) throws IgniteCheckedException {
        assert !grpDesc.sharedGroup() || ccfg.getGroupName() != null : ccfg.getName();

        File cacheWorkDir = ft.cacheStorage(ccfg);

        String dataRegionName = grpDesc.config().getDataRegionName();
        DataRegion dataRegion = cctx.database().dataRegion(dataRegionName);
        PageMetrics pageMetrics = dataRegion.metrics().cacheGrpPageMetrics(grpDesc.groupId());

        return initDir(
            cacheWorkDir,
            grpDesc.groupId(),
            ccfg.getName(),
            grpDesc.config().getAffinity().partitions(),
            pageMetrics,
            ccfg.isEncryptionEnabled(),
            grpDesc.caches().keySet()
        );
    }

    /**
     * @param grpId Cache group id.
     * @param encrypted {@code true} if cache group encryption enabled.
     * @return Factory to create page stores.
     */
    public FileVersionCheckingFactory getPageStoreFactory(int grpId, boolean encrypted) {
        return getPageStoreFactory(grpId, encrypted ? cctx.kernalContext().encryption() : null);
    }

    /**
     * @param grpId Cache group id.
     * @param encrKeyProvider Encryption keys provider for encrypted IO. If {@code null}, no encryption is used.
     * @return Factory to create page stores with certain encryption keys provider.
     */
    public FileVersionCheckingFactory getPageStoreFactory(int grpId, EncryptionCacheKeyProvider encrKeyProvider) {
        FileIOFactory pageStoreFileIoFactory = this.pageStoreFileIoFactory;
        FileIOFactory pageStoreV1FileIoFactory = this.pageStoreV1FileIoFactory;

        if (encrKeyProvider != null) {
            pageStoreFileIoFactory = encryptedFileIoFactory(this.pageStoreFileIoFactory, grpId, encrKeyProvider);
            pageStoreV1FileIoFactory = encryptedFileIoFactory(this.pageStoreV1FileIoFactory, grpId, encrKeyProvider);
        }

        FileVersionCheckingFactory pageStoreFactory = new FileVersionCheckingFactory(
            pageStoreFileIoFactory,
            pageStoreV1FileIoFactory,
            igniteCfg.getDataStorageConfiguration()::getPageSize
        );

        if (encrKeyProvider != null) {
            int hdrSize = pageStoreFactory.headerSize(pageStoreFactory.latestVersion());

            ((EncryptedFileIOFactory)pageStoreFileIoFactory).headerSize(hdrSize);
            ((EncryptedFileIOFactory)pageStoreV1FileIoFactory).headerSize(hdrSize);
        }

        return pageStoreFactory;
    }

    /**
     * @param plainFileIOFactory Not-encrypting file io factory.
     * @param cacheGrpId Cache group id.
     * @param encrKeyProvider Encryption keys provider for encrypted IO. If {@code null}, no encryption is used.
     * @return Encrypted file IO factory.
     */
    public EncryptedFileIOFactory encryptedFileIoFactory(FileIOFactory plainFileIOFactory, int cacheGrpId,
        EncryptionCacheKeyProvider encrKeyProvider) {
        return new EncryptedFileIOFactory(
            plainFileIOFactory,
            cacheGrpId,
            pageSize(),
            encrKeyProvider,
            cctx.gridConfig().getEncryptionSpi());
    }

    /**
     * @return Encrypted file IO factory with stored internal encryption keys.
     */
    public EncryptedFileIOFactory encryptedFileIoFactory(FileIOFactory plainFileIOFactory, int cacheGrpId) {
        return encryptedFileIoFactory(plainFileIOFactory, cacheGrpId, cctx.kernalContext().encryption());
    }

    /**
     * @param cacheWorkDir Work directory.
     * @param grpId Group ID.
     * @param cacheName Cache name.
     * @param partitions Number of partitions.
     * @param pageMetrics Page metrics.
     * @param encrypted {@code True} if this cache encrypted.
     * @return Cache store holder.
     * @throws IgniteCheckedException If failed.
     */
    private CacheStoreHolder initDir(File cacheWorkDir,
        int grpId,
        String cacheName,
        int partitions,
        PageMetrics pageMetrics,
        boolean encrypted,
        Collection<String> grpCaches) throws IgniteCheckedException {
        try {
            boolean dirExisted = checkAndInitCacheWorkDir(cacheWorkDir, log);

            if (dirExisted) {
                MaintenanceRegistry mntcReg = cctx.kernalContext().maintenanceRegistry();

                if (!mntcReg.isMaintenanceMode())
                    DefragmentationFileUtils.beforeInitPageStores(cacheWorkDir, log);
            }

            File idxFile = new File(cacheWorkDir, INDEX_FILE_NAME);

            GridQueryProcessor qryProc = cctx.kernalContext().query();

            if (qryProc.moduleEnabled()) {
                boolean idxRecreating = grpCaches == null
                    ? !qryProc.recreateCompleted(cacheName)
                    : grpCaches.stream().anyMatch(name -> !qryProc.recreateCompleted(name));

                if (idxFile.exists() && idxRecreating) {
                    log.warning("Recreate of index.bin don't finish before node stop, index.bin can be inconsistent. " +
                        "Removing it to recreate one more time [grpId=" + grpId + ", cacheName=" + cacheName + ']');

                    if (!idxFile.delete()) {
                        throw new IgniteCheckedException(
                            "Failed to remove index.bin [grpId=" + grpId + ", cacheName=" + cacheName + ']'
                        );
                    }
                }
            }

            if (dirExisted && !idxFile.exists())
                grpsWithoutIdx.add(grpId);

            FileVersionCheckingFactory pageStoreFactory = getPageStoreFactory(grpId, encrypted);

            PageStore idxStore =
                pageStoreFactory.createPageStore(
                    PageStore.TYPE_IDX,
                    idxFile,
                    pageMetrics.totalPages()::add);

            PageStore[] partStores = new PageStore[partitions];

            for (int partId = 0; partId < partStores.length; partId++) {
                final int p = partId;

                PageStore partStore =
                    pageStoreFactory.createPageStore(
                        PageStore.TYPE_DATA,
                        () -> getPartitionFilePath(cacheWorkDir, p),
                        pageMetrics.totalPages()::add);

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
        return new File(NodeFileTree.cacheStorage(workDir, cacheDirName), getPartitionFileName(partId));
    }

    /**
     * @param partId Partition id.
     * @return File name.
     */
    public static String getPartitionFileName(int partId) {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;

        return partId == INDEX_PARTITION ? INDEX_FILE_NAME : format(PART_FILE_TEMPLATE, partId);
    }

    /**
     * @param cacheWorkDir Cache work directory.
     */
    public static boolean checkAndInitCacheWorkDir(File cacheWorkDir, IgniteLogger log) throws IgniteCheckedException {
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

                Path cacheWorkDirPath = cacheWorkDir.toPath();

                Path tmp = cacheWorkDirPath.getParent().resolve(cacheWorkDir.getName() + TMP_SUFFIX);

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
        return pmPageMgr.allocatePage(grpId, partId, flags);
    }

    /** {@inheritDoc} */
    @Override public int pages(int grpId, int partId) throws IgniteCheckedException {
        PageStore store = getStore(grpId, partId);

        return store.pages();
    }

    /**
     * @param dir Directory to check.
     * @param names Cache group names to filter.
     * @return Files that match cache or cache group pattern.
     */
    public static List<File> cacheDirectories(File dir, Predicate<String> names) {
        File[] files = dir.listFiles();

        if (files == null)
            return Collections.emptyList();

        return Arrays.stream(files)
            .sorted()
            .filter(File::isDirectory)
            .filter(CACHE_DIR_WITH_META_FILTER)
            .filter(f -> names.test(NodeFileTree.cacheName(f)))
            .collect(Collectors.toList());
    }

    /**
     * @param dir Directory to check.
     * @param grpId Cache group id
     * @return Files that match cache or cache group pattern.
     */
    public static File cacheDirectory(File dir, int grpId) {
        File[] files = dir.listFiles();

        if (files == null)
            return null;

        return Arrays.stream(files)
            .filter(File::isDirectory)
            .filter(CACHE_DIR_WITH_META_FILTER)
            .filter(f -> CU.cacheId(NodeFileTree.cacheName(f)) == grpId)
            .findAny()
            .orElse(null);
    }

    /**
     * @param partFileName Partition file name.
     * @return Partition id.
     */
    public static int partId(String partFileName) {
        if (partFileName.equals(INDEX_FILE_NAME))
            return PageIdAllocator.INDEX_PARTITION;

        if (partFileName.startsWith(PART_FILE_PREFIX))
            return Integer.parseInt(partFileName.substring(PART_FILE_PREFIX.length(), partFileName.indexOf('.')));

        throw new IllegalStateException("Illegal partition file name: " + partFileName);
    }

    /**
     * @param cacheDir Cache directory to check.
     * @return List of cache partitions in given directory.
     */
    public static List<File> cachePartitionFiles(File cacheDir) {
        return cachePartitionFiles(cacheDir, FILE_SUFFIX);
    }

    /**
     * @param cacheDir Cache directory to check.
     * @param ext File extension.
     * @return List of cache partitions in given directory.
     */
    public static List<File> cachePartitionFiles(File cacheDir, String ext) {
        File[] files = cacheDir.listFiles();

        if (files == null)
            return Collections.emptyList();

        return Arrays.stream(files)
            .filter(File::isFile)
            .filter(f -> f.getName().endsWith(ext))
            .collect(Collectors.toList());
    }

    /**
     * @param root Root directory.
     * @return Array of cache data files.
     */
    public static File[] cacheDataFiles(File root) {
        return root.listFiles(f -> f.getName().endsWith(CACHE_DATA_FILENAME));
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
     * @param grpId Group id.
     * @return Name of cache group directory.
     * @throws IgniteCheckedException If cache group doesn't exist.
     */
    public String cacheDirName(int grpId) throws IgniteCheckedException {
        if (grpId == MetaStorage.METASTORAGE_CACHE_ID)
            return MetaStorage.METASTORAGE_DIR_NAME;

        CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

        if (gctx == null)
            throw new IgniteCheckedException("Cache group context has not found due to the cache group is stopped.");

        return ft.cacheDirName(gctx.config());
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
     * Return cache store holder.
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
        }
        catch (IgniteException ex) {
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
    @Override public Collection<PageStore> getStores(int grpId) throws IgniteCheckedException {
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
    @Override public PageStore getStore(int grpId, int partId) throws IgniteCheckedException {
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
