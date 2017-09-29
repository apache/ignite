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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
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
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * File page store manager.
 */
public class FilePageStoreManager extends GridCacheSharedManagerAdapter implements IgnitePageStoreManager {
    /** File suffix. */
    public static final String FILE_SUFFIX = ".bin";

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
    public static final String DFLT_STORE_DIR = "db";

    /** Marshaller. */
    private static final Marshaller marshaller = new JdkMarshaller();

    /** */
    private final Map<Integer, CacheStoreHolder> idxCacheStores = new ConcurrentHashMap<>();

    /** */
    private final IgniteConfiguration igniteCfg;

    /** */
    private PersistentStoreConfiguration pstCfg;

    /** Absolute directory for file page store */
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

        PersistentStoreConfiguration pstCfg = igniteCfg.getPersistentStoreConfiguration();

        assert pstCfg != null : "WAL should not be created if persistence is disabled.";

        this.pstCfg = pstCfg;
    }

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        if (cctx.kernalContext().clientNode())
            return;

        String consId = U.maskForFileName(cctx.kernalContext().discovery().consistentId().toString());

        if (pstCfg.getPersistentStorePath() != null) {
            File workDir0 = new File(pstCfg.getPersistentStorePath());

            if (!workDir0.isAbsolute())
                workDir0 = U.resolveWorkDirectory(
                    igniteCfg.getWorkDirectory(),
                    pstCfg.getPersistentStorePath(),
                    false
                );

            storeWorkDir = new File(workDir0, consId);
        }
        else
            storeWorkDir = new File(U.resolveWorkDirectory(
                igniteCfg.getWorkDirectory(),
                DFLT_STORE_DIR,
                false
            ), consId);

        U.ensureDirectory(storeWorkDir, "page store work directory", log);
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

            for (FilePageStore partStore : holder.partStores)
                partStore.beginRecover();
        }
    }

    /** {@inheritDoc} */
    @Override public void finishRecover() {
        for (CacheStoreHolder holder : idxCacheStores.values()) {
            holder.idxStore.finishRecover();

            for (FilePageStore partStore : holder.partStores)
                partStore.finishRecover();
        }
    }

    /** {@inheritDoc} */
    @Override public void initializeForCache(CacheGroupDescriptor grpDesc, StoredCacheData cacheData)
        throws IgniteCheckedException {
        int grpId = grpDesc.groupId();

        if (!idxCacheStores.containsKey(grpId)) {
            CacheStoreHolder holder = initForCache(grpDesc, cacheData.config());

            CacheStoreHolder old = idxCacheStores.put(grpId, holder);

            assert old == null : "Non-null old store holder for cache: " + cacheData.config().getName();
        }
    }

    /** {@inheritDoc} */
    @Override public void storeCacheData(StoredCacheData cacheData, boolean overwrite) throws IgniteCheckedException {
        File cacheWorkDir = cacheWorkDirectory(cacheData.config());
        File file;

        checkAndInitCacheWorkDir(cacheWorkDir);

        assert cacheWorkDir.exists() : "Work directory does not exist: " + cacheWorkDir;

        if (cacheData.config().getGroupName() != null)
            file = new File(cacheWorkDir, cacheData.config().getName() + CACHE_DATA_FILENAME);
        else
            file = new File(cacheWorkDir, CACHE_DATA_FILENAME);

        if (overwrite || !file.exists() || file.length() == 0) {
            try {
                file.createNewFile();

                // Pre-existing file will be truncated upon stream open.
                try (OutputStream stream = new BufferedOutputStream(new FileOutputStream(file))) {
                    marshaller.marshal(cacheData, stream);
                }
            }
            catch (IOException ex) {
                throw new IgniteCheckedException("Failed to persist cache configuration: " + cacheData.config().getName(), ex);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void shutdownForCacheGroup(CacheGroupContext grp, boolean destroy) throws IgniteCheckedException {
        grpsWithoutIdx.remove(grp.groupId());

        CacheStoreHolder old = idxCacheStores.remove(grp.groupId());

        assert old != null : "Missing cache store holder [cache=" + grp.cacheOrGroupName() +
            ", locNodeId=" + cctx.localNodeId() + ", gridName=" + cctx.igniteInstanceName() + ']';

        IgniteCheckedException ex = shutdown(old, /*clean files if destroy*/destroy, null);

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

        assert store instanceof FilePageStore : store;

        ((FilePageStore)store).truncate(tag);
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

        store.read(pageId, pageBuf, keepCrc);
    }

    /** {@inheritDoc} */
    @Override public boolean exists(int grpId, int partId) throws IgniteCheckedException {
        PageStore store = getStore(grpId, partId);

        return store.exists();
    }

    /** {@inheritDoc} */
    @Override public void readHeader(int grpId, int partId, ByteBuffer buf) throws IgniteCheckedException {
        PageStore store = getStore(grpId, partId);

        store.readHeader(buf);
    }

    /** {@inheritDoc} */
    @Override public void write(int grpId, long pageId, ByteBuffer pageBuf, int tag) throws IgniteCheckedException {
        writeInternal(grpId, pageId, pageBuf, tag);
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
     * @return PageStore to which the page has been written.
     * @throws IgniteCheckedException If IO error occurred.
     */
    public PageStore writeInternal(int cacheId, long pageId, ByteBuffer pageBuf, int tag) throws IgniteCheckedException {
        int partId = PageIdUtils.partId(pageId);

        PageStore store = getStore(cacheId, partId);

        store.write(pageId, pageBuf, tag);

        return store;
    }

    /**
     * @param ccfg Cache configuration.
     * @return Cache work directory.
     */
    private File cacheWorkDirectory(CacheConfiguration ccfg) {
        String dirName;

        if (ccfg.getGroupName() != null)
            dirName = CACHE_GRP_DIR_PREFIX + ccfg.getGroupName();
        else
            dirName = CACHE_DIR_PREFIX + ccfg.getName();

        return new File(storeWorkDir, dirName);
    }

    /**
     * @param grpDesc Cache group descriptor.
     * @param ccfg Cache configuration.
     * @return Cache store holder.
     * @throws IgniteCheckedException If failed.
     */
    private CacheStoreHolder initForCache(CacheGroupDescriptor grpDesc, CacheConfiguration ccfg) throws IgniteCheckedException {
        assert !grpDesc.sharedGroup() || ccfg.getGroupName() != null : ccfg.getName();

        File cacheWorkDir = cacheWorkDirectory(ccfg);

        boolean dirExisted = checkAndInitCacheWorkDir(cacheWorkDir);

        File idxFile = new File(cacheWorkDir, INDEX_FILE_NAME);

        if (dirExisted && !idxFile.exists())
            grpsWithoutIdx.add(grpDesc.groupId());

        FileVersionCheckingFactory pageStoreFactory = new FileVersionCheckingFactory(
            pstCfg.getFileIOFactory(), igniteCfg.getMemoryConfiguration());

        FilePageStore idxStore = pageStoreFactory.createPageStore(PageMemory.FLAG_IDX, idxFile);

        FilePageStore[] partStores = new FilePageStore[grpDesc.config().getAffinity().partitions()];

        for (int partId = 0; partId < partStores.length; partId++) {
            FilePageStore partStore = pageStoreFactory.createPageStore(
                PageMemory.FLAG_DATA, new File(cacheWorkDir, String.format(PART_FILE_TEMPLATE, partId)));

            partStores[partId] = partStore;
        }

        return new CacheStoreHolder(idxStore, partStores);
    }

    /**
     * @param cacheWorkDir Cache work directory.
     */
    private boolean checkAndInitCacheWorkDir(File cacheWorkDir) throws IgniteCheckedException {
        boolean dirExisted = false;

        if (!cacheWorkDir.exists()) {
            boolean res = cacheWorkDir.mkdirs();

            if (!res)
                throw new IgniteCheckedException("Failed to initialize cache working directory " +
                    "(failed to create, make sure the work folder has correct permissions): " +
                    cacheWorkDir.getAbsolutePath());
        }
        else {
            if (cacheWorkDir.isFile())
                throw new IgniteCheckedException("Failed to initialize cache working directory " +
                    "(a file with the same name already exists): " + cacheWorkDir.getAbsolutePath());

            File lockF = new File(cacheWorkDir, IgniteCacheSnapshotManager.SNAPSHOT_RESTORE_STARTED_LOCK_FILENAME);

            if (lockF.exists()) {
                Path tmp = cacheWorkDir.toPath().getParent().resolve(cacheWorkDir.getName() + ".tmp");

                boolean deleted = U.delete(cacheWorkDir);

                if (Files.exists(tmp) && Files.isDirectory(tmp)) {
                    U.warn(log, "Ignite node crashed during the snapshot restore process " +
                        "(there is a snapshot restore lock file left for cache). But old version of cache was saved. " +
                        "Trying to restore it. Cache - [" + cacheWorkDir.getAbsolutePath() + ']');

                    try {
                        Files.move(tmp, cacheWorkDir.toPath());
                    }
                    catch (IOException e) {
                        throw new IgniteCheckedException(e);
                    }
                }
                else {
                    U.warn(log, "Ignite node crashed during the snapshot restore process " +
                        "(there is a snapshot restore lock file left for cache). Will remove both the lock file and " +
                        "incomplete cache directory [cacheDir=" + cacheWorkDir.getAbsolutePath() + ']');

                    if (!deleted)
                        throw new IgniteCheckedException("Failed to remove obsolete cache working directory " +
                            "(remove the directory manually and make sure the work folder has correct permissions): " +
                            cacheWorkDir.getAbsolutePath());

                    cacheWorkDir.mkdirs();
                }

                if (!cacheWorkDir.exists())
                    throw new IgniteCheckedException("Failed to initialize cache working directory " +
                        "(failed to create, make sure the work folder has correct permissions): " +
                        cacheWorkDir.getAbsolutePath());
            }
            else
                dirExisted = true;
        }

        return dirExisted;
    }

    /** {@inheritDoc} */
    @Override public void sync(int grpId, int partId) throws IgniteCheckedException {
        getStore(grpId, partId).sync();
    }

    /** {@inheritDoc} */
    @Override public void ensure(int grpId, int partId) throws IgniteCheckedException {
        getStore(grpId, partId).ensure();
    }

    /** {@inheritDoc} */
    @Override public long allocatePage(int grpId, int partId, byte flags) throws IgniteCheckedException {
        assert partId <= PageIdAllocator.MAX_PARTITION_ID || partId == PageIdAllocator.INDEX_PARTITION;

        PageStore store = getStore(grpId, partId);

        long pageIdx = store.allocatePage();

        return PageIdUtils.pageId(partId, flags, (int)pageIdx);
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

        for (File file : files) {
            if (file.isDirectory()) {
                if (file.getName().startsWith(CACHE_DIR_PREFIX)) {
                    File conf = new File(file, CACHE_DATA_FILENAME);

                    if (conf.exists() && conf.length() > 0) {
                        StoredCacheData cacheData = readCacheData(conf);

                        ccfgs.put(cacheData.config().getName(), cacheData);
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

                ccfgs.put(cacheData.config().getName(), cacheData);
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
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to read cache configuration from disk for cache: " +
                conf.getAbsolutePath(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasIndexStore(int grpId) {
        return !grpsWithoutIdx.contains(grpId);
    }

    /**
     * @return Store work dir.
     */
    public File workDir() {
        return storeWorkDir;
    }

    /**
     * @param ccfg Cache configuration.
     * @return Store dir for given cache.
     */
    public File cacheWorkDir(CacheConfiguration ccfg) {
        String dirName = ccfg.getGroupName() == null ?
            CACHE_DIR_PREFIX + ccfg.getName() : CACHE_GRP_DIR_PREFIX + ccfg.getGroupName();

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

        for (FilePageStore store : holder.partStores) {
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
    private IgniteCheckedException shutdown(FilePageStore store, boolean cleanFile, IgniteCheckedException aggr) {
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

        FilePageStore store = holder.partStores[partId];

        if (store == null)
            throw new IgniteCheckedException("Failed to get page store for the given partition ID " +
                "(partition has not been created) [grpId=" + grpId + ", partId=" + partId + ']');

        return store;
    }

    /**
     *
     */
    private static class CacheStoreHolder {
        /** Index store. */
        private final FilePageStore idxStore;

        /** Partition stores. */
        private final FilePageStore[] partStores;

        /**
         *
         */
        public CacheStoreHolder(FilePageStore idxStore, FilePageStore[] partStores) {
            this.idxStore = idxStore;
            this.partStores = partStores;
        }
    }

}
