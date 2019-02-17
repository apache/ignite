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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.AllocatedPageTracker;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.Files.delete;
import static java.nio.file.Files.newDirectoryStream;

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
    public static final String PART_FILE_TEMPLATE = PART_FILE_PREFIX+ "%d" + FILE_SUFFIX;

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

    /** Marshaller. */
    private static final Marshaller marshaller = new JdkMarshaller();

    /** */
    private final Map<Integer, CacheStoreHolder> idxCacheStores = new ConcurrentHashMap<>();

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

    /**
     * @param ctx Kernal context.
     */
    public FilePageStoreManager(GridKernalContext ctx) {
        igniteCfg = ctx.config();

        DataStorageConfiguration dsCfg = igniteCfg.getDataStorageConfiguration();

        assert dsCfg != null;

        this.dsCfg = dsCfg;

        pageStoreV1FileIoFactory = pageStoreFileIoFactory = dsCfg.getFileIOFactory();
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

        if (tmpDir != null && storeWorkDir.getAbsolutePath().contains(tmpDir)) {
            log.warning("Persistence store directory is in the temp directory and may be cleaned." +
                "To avoid this set \"IGNITE_HOME\" environment variable properly or " +
                "change location of persistence directories in data storage configuration " +
                "(see DataStorageConfiguration#walPath, DataStorageConfiguration#walArchivePath, " +
                "DataStorageConfiguration#storagePath properties). " +
                "Current persistence store directory is: [" + tmpDir + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public void cleanupPersistentSpace(CacheConfiguration cacheConfiguration) throws IgniteCheckedException {
        try {
            File cacheWorkDir = cacheWorkDir(cacheConfiguration);

            if(!cacheWorkDir.exists())
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
    @Override public void stop0(boolean cancel) {
        if (log.isDebugEnabled())
            log.debug("Stopping page store manager.");

        IgniteCheckedException ex = shutdown(false);

        if (ex != null)
            U.error(log, "Failed to gracefully stop page store manager", ex);
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

        idxCacheStores.clear();
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
    @Override public void initialize(int cacheId, int partitions, String workingDir, AllocatedPageTracker tracker)
        throws IgniteCheckedException {
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
        int grpId = grpDesc.groupId();

        if (!idxCacheStores.containsKey(grpId)) {
            CacheStoreHolder holder = initForCache(grpDesc, cacheData.config());

            CacheStoreHolder old = idxCacheStores.put(grpId, holder);

            assert old == null : "Non-null old store holder for cache: " + cacheData.config().getName();
        }
    }

    /** {@inheritDoc} */
    @Override public void initializeForMetastorage() throws IgniteCheckedException {
        int grpId = MetaStorage.METASTORAGE_CACHE_ID;

        if (!idxCacheStores.containsKey(grpId)) {
            CacheStoreHolder holder = initDir(
                new File(storeWorkDir, META_STORAGE_NAME),
                    grpId,
                    1,
                    AllocatedPageTracker.NO_OP,
                    false);

            CacheStoreHolder old = idxCacheStores.put(grpId, holder);

            assert old == null : "Non-null old store holder for metastorage";
        }
    }

    /** {@inheritDoc} */
    @Override public void storeCacheData(StoredCacheData cacheData, boolean overwrite) throws IgniteCheckedException {
        File cacheWorkDir = cacheWorkDir(cacheData.config());
        File file;

        checkAndInitCacheWorkDir(cacheWorkDir);

        assert cacheWorkDir.exists() : "Work directory does not exist: " + cacheWorkDir;

        if (cacheData.config().getGroupName() != null)
            file = new File(cacheWorkDir, cacheData.config().getName() + CACHE_DATA_FILENAME);
        else
            file = new File(cacheWorkDir, CACHE_DATA_FILENAME);

        Path filePath = file.toPath();

        try {
            if (overwrite || !Files.exists(filePath) || Files.size(filePath) == 0) {
                File tmp = new File(file.getParent(), file.getName() + TMP_SUFFIX);

                tmp.createNewFile();

                // Pre-existing file will be truncated upon stream open.
                try (OutputStream stream = new BufferedOutputStream(new FileOutputStream(tmp))) {
                    marshaller.marshal(cacheData, stream);
                }

                if (file.exists())
                    file.delete();

                Files.move(tmp.toPath(), file.toPath());
            }
        }
        catch (IOException ex) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, ex));

            throw new IgniteCheckedException("Failed to persist cache configuration: " + cacheData.config().getName(), ex);
        }
    }

    /** {@inheritDoc} */
    @Override public void shutdownForCacheGroup(CacheGroupContext grp, boolean destroy) throws IgniteCheckedException {
        grpsWithoutIdx.remove(grp.groupId());

        CacheStoreHolder old = idxCacheStores.remove(grp.groupId());

        assert old != null : "Missing cache store holder [cache=" + grp.cacheOrGroupName() +
            ", locNodeId=" + cctx.localNodeId() + ", gridName=" + cctx.igniteInstanceName() + ']';

        IgniteCheckedException ex = shutdown(old, /*clean files if destroy*/destroy, null);

        if (destroy)
            removeCacheGroupConfigurationData(grp);

        if (ex != null)
            throw ex;
    }

    /** {@inheritDoc} */
    @Override public void onPartitionCreated(int grpId, int partId) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onPartitionDestroyed(int grpId, int partId, int tag) throws IgniteCheckedException {
        assert partId <= PageIdAllocator.MAX_PARTITION_ID;

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
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param pageBuf Page buffer.
     * @param keepCrc Keep CRC flag.
     * @throws IgniteCheckedException If failed.
     */
    public void read(int cacheId, long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteCheckedException {
        PageStore store = getStore(cacheId, PageIdUtils.partId(pageId));

        try {
            store.read(pageId, pageBuf, keepCrc);
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
    public PageStore writeInternal(int cacheId, long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) throws IgniteCheckedException {
        int partId = PageIdUtils.partId(pageId);

        PageStore store = getStore(cacheId, partId);

        try {
            store.write(pageId, pageBuf, tag, calculateCrc);
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
        return getPartitionFile(cacheWorkDir(isSharedGroup, cacheOrGroupName), partId).toPath();
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

        int grpId = CU.cacheId(grpDesc.cacheOrGroupName());

        AllocatedPageTracker allocatedTracker = regionMetrics.getOrAllocateGroupPageAllocationTracker(grpId);

        return initDir(
            cacheWorkDir,
            grpDesc.groupId(),
            grpDesc.config().getAffinity().partitions(),
            allocatedTracker,
            ccfg.isEncryptionEnabled()
        );
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
        AllocatedPageTracker allocatedTracker,
        boolean encrypted) throws IgniteCheckedException {
        try {
            boolean dirExisted = checkAndInitCacheWorkDir(cacheWorkDir);

            File idxFile = new File(cacheWorkDir, INDEX_FILE_NAME);

            if (dirExisted && !idxFile.exists())
                grpsWithoutIdx.add(grpId);


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
                igniteCfg.getDataStorageConfiguration());

            if (encrypted) {
                int headerSize = pageStoreFactory.headerSize(pageStoreFactory.latestVersion());

                ((EncryptedFileIOFactory)pageStoreFileIoFactory).headerSize(headerSize);
                ((EncryptedFileIOFactory)pageStoreV1FileIoFactory).headerSize(headerSize);
            }

            PageStore idxStore =
            pageStoreFactory.createPageStore(
                PageMemory.FLAG_IDX,
                idxFile,
                allocatedTracker);

            PageStore[] partStores = new PageStore[partitions];

            for (int partId = 0; partId < partStores.length; partId++) {
                PageStore partStore =
                    pageStoreFactory.createPageStore(
                        PageMemory.FLAG_DATA,
                        getPartitionFile(cacheWorkDir, partId),
                        allocatedTracker);

                    partStores[partId] = partStore;
                }

            return new CacheStoreHolder(idxStore, partStores);
        }
        catch (StorageException e) {
            cctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));

            throw e;
        }
    }

    /**
     * @param cacheWorkDir Cache work directory.
     * @param partId Partition id.
     */
    @NotNull private File getPartitionFile(File cacheWorkDir, int partId) {
        return new File(cacheWorkDir, String.format(PART_FILE_TEMPLATE, partId));
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
        assert partId <= PageIdAllocator.MAX_PARTITION_ID || partId == PageIdAllocator.INDEX_PARTITION;

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
                File[] tmpFiles = file.listFiles(new FilenameFilter() {
                    @Override public boolean accept(File dir, String name) {
                        return name.endsWith(CACHE_DATA_TMP_FILENAME);
                    }
                });

                if (tmpFiles != null) {
                    for (File tmpFile: tmpFiles) {
                        if (!tmpFile.delete())
                            log.warning("Failed to delete temporary cache config file" +
                                    "(make sure Ignite process has enough rights):" + file.getName());
                    }
                }

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
    public File cacheWorkDir(CacheConfiguration ccfg) {
        boolean isSharedGrp = ccfg.getGroupName() != null;

        return cacheWorkDir(isSharedGrp, isSharedGrp ? ccfg.getGroupName() : ccfg.getName());
    }

    /**
     *
     */
    public File cacheWorkDir(boolean isSharedGroup, String cacheOrGroupName) {
        String dirName;

        if (isSharedGroup)
            dirName = CACHE_GRP_DIR_PREFIX + cacheOrGroupName;
        else
            dirName = CACHE_DIR_PREFIX + cacheOrGroupName;

        return new File(storeWorkDir, dirName);
    }

    /**
     * @param cleanFiles {@code True} if the stores should delete it's files upon close.
     */
    private IgniteCheckedException shutdown(boolean cleanFiles) {
        IgniteCheckedException ex = null;

        for (CacheStoreHolder holder : idxCacheStores.values())
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
                for(Path path: dirStream)
                    Files.deleteIfExists(path);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to delete cache configurations of group: " + ctx.toString(), e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void removeCacheData(StoredCacheData cacheData) throws IgniteCheckedException {
        CacheConfiguration cacheCfg = cacheData.config();
        File cacheWorkDir = cacheWorkDir(cacheCfg);
        File file;

        if (cacheData.config().getGroupName() != null)
            file = new File(cacheWorkDir, cacheCfg.getName() + CACHE_DATA_FILENAME);
        else
            file = new File(cacheWorkDir, CACHE_DATA_FILENAME);

        if (file.exists()) {
            if (!file.delete())
                throw new IgniteCheckedException("Failed to delete cache configuration:" + cacheCfg.getName());
        }
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
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @return Page store for the corresponding parameters.
     * @throws IgniteCheckedException If cache or partition with the given ID was not created.
     *
     * Note: visible for testing.
     */
    public PageStore getStore(int grpId, int partId) throws IgniteCheckedException {
        CacheStoreHolder holder = idxCacheStores.get(grpId);

        if (holder == null)
            throw new IgniteCheckedException("Failed to get page store for the given cache ID " +
                "(cache has not been started): " + grpId);

        if (partId == PageIdAllocator.INDEX_PARTITION)
            return holder.idxStore;

        if (partId > PageIdAllocator.MAX_PARTITION_ID)
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
    private static class CacheStoreHolder {
        /** Index store. */
        private final PageStore idxStore;

        /** Partition stores. */
        private final PageStore[] partStores;

        /**
         *
         */
        public CacheStoreHolder(PageStore idxStore, PageStore[] partStores) {
            this.idxStore = idxStore;
            this.partStores = partStores;
        }
    }
}
