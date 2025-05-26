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

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.cdc.CdcManager;
import org.apache.ignite.internal.cdc.CdcMode;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_CDC_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_NAME;

/**
 * Provides access to Ignite node file tree.
 * Note, that root path can be different for each usage:
 * <ul>
 *     <li>Ignite node.</li>
 *     <li>Snapshot files.</li>
 *     <li>Cache dump files.</li>
 *     <li>CDC.</li>
 * </ul>
 *
 * Ignite node file tree structure with the point to currenlty supported dirs.
 * Description:<br>
 * <ul>
 *     <li>{@code .} folder is {@code root} constructor parameter.</li>
 *     <li>{@code node00-e57e62a9-2ccf-4e1b-a11e-c24c21b9ed4c} is the {@link PdsFolderSettings#folderName()} and {@code folderName}
 *     constructor parameter.</li>
 *     <li>{@code db/binary_meta}, {@code db/marshaller} directories calculated relative to Ignite working directory.</li>
 *     <li>{@code nodeStorage} calculated relative to {@link DataStorageConfiguration#getStoragePath()},
 *     which equal to {@code ${IGNITE_WORK_DIR}/db}, by default.</li>
 * </ul>
 *
 * <pre>
 * ❯ tree
 * .                                                                            ← root (work directory, shared between all local nodes).
 * ├── cp                                                                       ← default sharedfs root. See  {@link SharedFsCheckpointSpi}.
 * │  └── sharedfs
 * │      └── BinaryMarshaller
 * ├── db                                                                       ← db (shared between all local nodes).
 * │  ├── binary_meta                                                           ← binaryMetaRoot (shared between all local nodes).
 * │  │  └── node00-e57e62a9-2ccf-4e1b-a11e-c24c21b9ed4c                        ← binaryMeta for node 0
 * │  │      └── 1645778359.bin
 * │  │  └── node01-e57e62a9-2ccf-4e1b-a11e-d35d32c0fe5d                        ← binaryMeta for node 1
 * │  │      └── 1645778359.bin
 * │  ├── lock
 * │  ├── marshaller                                                            ← marshaller (shared between all local nodes)
 * │  │  └── 1645778359.classname0
 * │  ├── node00-e57e62a9-2ccf-4e1b-a11e-c24c21b9ed4c                           ← nodeStorage (node 0).
 * │  │  ├── cache-default                                                      ← cacheStorage (cache name "default").
 * │  │  │  ├── cache_data.dat                                                  ← cache("default") configuration file.
 * │  │  │  ├── index.bin                                                       ← cache("default") index partition.
 * │  │  │  ├── part-0.bin                                                      ← cache("default") partition 0.
 * │  │  │  ├── part-1.bin
 * ...
 * │  │  │  └── part-9.bin
 * │  │  ├── cache-ignite-sys-cache                                             ← cacheStorage (cache name "ignite-sys-cache").
 * │  │  │  ├── cache_data.dat                                                  ← cache("ignite-sys-cache") configuration file.
 * │  │  │  └── index.bin
 * │  │  ├── cacheGroup-tx-cache                                                ← cacheStorage (cache group "tx-cache").
 * │  │  │  ├── tx-cachecache_data.dat                                          ← cache("tx-cache") configuration file inside group.
 * │  │  │  ├── othercache_data.dat                                             ← cache("other") configuration file inside group.
 * │  │  │  ├── index.bin                                                       ← cache group ("tx-cache") index partition.
 * │  │  │  ├── part-0.bin                                                      ← cache group ("tx-cache") partition 0.
 * │  │  │  ├── part-1.bin
 * ...
 * │  │  │  └── part-9.bin
 * │  │  ├── cp                                                                 ← checkpoint (node 0).
 * │  │  │  ├── 1737804007693-96128bb0-5361-495a-b593-53dc4339a56d-END.bin
 * │  │  │  └── 1737804007693-96128bb0-5361-495a-b593-53dc4339a56d-START.bin
 * │  │  ├── lock
 * │  │  ├── maintenance_tasks.mntc
 * │  │  ├── metastorage
 * │  │  │  ├── part-0.bin
 * │  │  │  └── part-1.bin
 * │  │  └── snp                                                                ← snpTmp (node 0)
 * │  ├── node01-e57e62a9-2ccf-4e1b-a11e-d35d32c0fe5d                           ← nodeStorage (node 1).
 * │  │  ├── cache-default                                                      ← cacheStorage (cache name "default").
 * ..
 * │  │  ├── cache-ignite-sys-cache                                             ← cacheStorage (cache name "ignite-sys-cache").
 * ...
 * │  │  ├── cacheGroup-tx-cache                                                ← cacheStorage (cache group "tx-cache").
 * ...
 * │  │  ├── cp                                                                 ← checkpoint (node 1).
 * ...
 * │  │  ├── lock
 * │  │  ├── maintenance_tasks.mntc
 * │  │  ├── metastorage
 * ...
 * │  │  └── snp                                                                ← snpTmp (node 1)
 * ...
 * ...
 * │  └── wal
 * │      ├── archive
 * │      │  └── ignite_0
 * │      │      └── node00-e57e62a9-2ccf-4e1b-a11e-c24c21b9ed4c                ← walArchive (node 0)
 * │      │      └── node01-e57e62a9-2ccf-4e1b-a11e-d35d32c0fe5d                ← walArchive (node 1)
 * │      ├── cdc
 * │      │  └── node00-e57e62a9-2ccf-4e1b-a11e-c24c21b9ed4c                    ← walCdc (node 0)
 * │      │      ├── lock
 * │      │      └── state
 * │      │          ├── cdc-caches-state.bin
 * │      │          ├── cdc-mappings-state.bin
 * │      │          └── cdc-types-state.bin
 * ...
 * │      │  └── node01-e57e62a9-2ccf-4e1b-a11e-d35d32c0fe5d                    ← walCdc (node 1)
 * │      └── node00-e57e62a9-2ccf-4e1b-a11e-c24c21b9ed4c                       ← wal (node 0)
 * │          ├── 0000000000000000.wal                                          ← wal segment (index = 0)
 * │          ├── 0000000000000001.wal                                          ← wal segment (index = 1)
 * ...
 * │          └── 0000000000000009.wal                                          ← wal segment (index = 9)
 * │      └── node01-e57e62a9-2ccf-4e1b-a11e-d35d32c0fe5d                       ← wal (node 1)
 * ...
 * ├── diagnostic
 * ├── log
 * │  ├── all.log
 * │  ├── consistency.log
 * │  ├── filtered.log
 * │  ├── ignite-e10fbb91.0.log
 * │  ├── ignite.log
 * │  ├── jmx-invoker.0.log
 * ...
 * │  └── jmx-invoker.9.log
 * └── snapshots                                                                ← snpsRoot (shared between all local nodes).
 * </pre>
 */
public class NodeFileTree extends SharedFileTree {
    /** Default snapshot directory for loading remote snapshots. */
    protected static final String SNAPSHOT_TMP_DIR = "snp";

    /** Metastorage cache directory to store data. */
    private static final String METASTORAGE_DIR_NAME = "metastorage";

    /** Checkpoint directory name. */
    private static final String CHECKPOINT_DIR = "cp";

    /** File extension of WAL segment. */
    public static final String WAL_SEGMENT_FILE_EXT = ".wal";

    /** File suffix. */
    static final String FILE_SUFFIX = ".bin";

    /** Suffix for tmp files */
    public static final String TMP_SUFFIX = ".tmp";

    /** Suffix for zip files */
    public static final String ZIP_SUFFIX = ".zip";

    /** File extension of temp WAL segment. */
    public static final String TMP_WAL_SEG_FILE_EXT = WAL_SEGMENT_FILE_EXT + TMP_SUFFIX;

    /** File extension of zipped WAL segment. */
    public static final String ZIP_WAL_SEG_FILE_EXT = WAL_SEGMENT_FILE_EXT + ZIP_SUFFIX;

    /** File extension of temp zipped WAL segment. */
    public static final String TMP_ZIP_WAL_SEG_FILE_EXT = ZIP_WAL_SEG_FILE_EXT + TMP_SUFFIX;

    /** Filter out all cache directories. */
    private static final Predicate<File> CACHE_DIR_FILTER = dir -> cacheDir(dir) || cacheGroupDir(dir);

    /** Filter out all cache directories including {@link MetaStorage}. */
    private static final Predicate<File> CACHE_DIR_WITH_META_FILTER = dir ->
        CACHE_DIR_FILTER.test(dir) ||
            dir.getName().equals(METASTORAGE_DIR_NAME);

    /** Partition file prefix. */
    static final String PART_FILE_PREFIX = "part-";

    /** Index file prefix. */
    static final String INDEX_FILE_PREFIX = "index";

    /** Index file name. */
    protected static final String INDEX_FILE_NAME = INDEX_FILE_PREFIX + FILE_SUFFIX;

    /** Partition file template. */
    protected static final String PART_FILE_TEMPLATE = PART_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** */
    private static final String CACHE_DATA_FILENAME = "cache_data.dat";

    /** */
    private static final String CACHE_DATA_TMP_FILENAME = CACHE_DATA_FILENAME + TMP_SUFFIX;

    /** Temporary cache directory prefix. */
    private static final String TMP_CACHE_DIR_PREFIX = "_tmp_snp_restore_";

    /** Prefix for {@link #cacheStorage(CacheConfiguration)} directory in case of single cache. */
    private static final String CACHE_DIR_PREFIX = "cache-";

    /** Prefix for {@link #cacheStorage(CacheConfiguration)} directory in case of cache group. */
    private static final String CACHE_GRP_DIR_PREFIX = "cacheGroup-";

    /** CDC state directory name. */
    private static final String STATE_DIR = "state";

    /**
     * The file stores state of CDC mode. Content of the file is a {@link CdcMode} value:
     * <ul>
     *     <li>{@link CdcMode#CDC_UTILITY_ACTIVE} means that {@link CdcMain} utility captures data.</li>
     *     <li>{@link CdcMode#IGNITE_NODE_ACTIVE} means that {@link CdcManager} captures data within Ignite node.</li>
     * </ul>
     */
    private static final String CDC_MODE_FILE_NAME = "cdc-mode" + FILE_SUFFIX;

    /** WAL state file name. */
    private static final String WAL_STATE_FILE_NAME = "cdc-wal-state" + FILE_SUFFIX;

    /** Types state file name. */
    private static final String TYPES_STATE_FILE_NAME = "cdc-types-state" + FILE_SUFFIX;

    /** Mappings state file name. */
    private static final String MAPPINGS_STATE_FILE_NAME = "cdc-mappings-state" + FILE_SUFFIX;

    /** Caches state file name. */
    private static final String CACHES_STATE_FILE_NAME = "cdc-caches-state" + FILE_SUFFIX;

    /** Folder name for consistent id. */
    private final String folderName;

    /** Path to the directory containing binary metadata. */
    private final File binaryMeta;

    /** Path to the storage directory. */
    private final File nodeStorage;

    /**
     * Key is the path from {@link DataStorageConfiguration#getExtraStoragePathes()}, may be relative. Value is storage.
     * @see DataStorageConfiguration#getExtraStoragePathes()
     */
    protected final Map<String, File> extraStorages;

    /** Path to the checkpoint directory. */
    private final File checkpoint;

    /** Path to the directory containing active WAL segments. */
    private final File wal;

    /** Path to the directory containing archive WAL segments. */
    private final File walArchive;

    /** Path to the directory containing archive WAL segments for CDC. */
    private final File walCdc;

    /**
     * Root directory can be Ignite work directory or snapshot root, see {@link U#workDirectory(String, String)} and other methods.
     *
     * @param root Root directory.
     * @param folderName Name of the folder for current node.
     *                   Usually, it a {@link IgniteConfiguration#getConsistentId()} masked to be correct file name.
     *
     * @see IgniteConfiguration#getWorkDirectory()
     * @see IgniteConfiguration#setWorkDirectory(String)
     * @see U#workDirectory(String, String)
     * @see U#resolveWorkDirectory(String, String, boolean, boolean)
     * @see U#IGNITE_WORK_DIR
     */
    public NodeFileTree(File root, String folderName) {
        super(root);

        A.notNullOrEmpty(folderName, "Node directory");

        this.folderName = folderName;

        binaryMeta = new File(binaryMetaRoot, folderName);
        nodeStorage = rootRelative(DB_DIR);
        checkpoint = new File(nodeStorage, CHECKPOINT_DIR);
        wal = rootRelative(DFLT_WAL_PATH);
        walArchive = rootRelative(DFLT_WAL_ARCHIVE_PATH);
        walCdc = rootRelative(DFLT_WAL_CDC_PATH);
        extraStorages = Collections.emptyMap();
    }

    /**
     * Creates instance based on config and folder name.
     *
     * @param cfg Ignite configuration to get parameter from.
     * @param folderName Name of the folder for current node.
     *                   Usually, it a {@link IgniteConfiguration#getConsistentId()} masked to be correct file name.
     *
     * @see IgniteConfiguration#getWorkDirectory()
     * @see IgniteConfiguration#setWorkDirectory(String)
     * @see U#workDirectory(String, String)
     * @see U#resolveWorkDirectory(String, String, boolean, boolean)
     * @see U#IGNITE_WORK_DIR
     */
    public NodeFileTree(IgniteConfiguration cfg, String folderName) {
        this(cfg, resolveRoot(cfg), folderName, false);
    }

    /**
     * Creates instance based on config and folder name.
     *
     * @param root Root directory.
     * @param cfg Ignite configuration to get parameter from.
     * @param folderName Name of the folder for current node.
     *                   Usually, it a {@link IgniteConfiguration#getConsistentId()} masked to be correct file name.
     * @param isSnapshot {@code True} if tree relfects snapshot structure.
     *
     * @see IgniteConfiguration#getWorkDirectory()
     * @see IgniteConfiguration#setWorkDirectory(String)
     * @see U#workDirectory(String, String)
     * @see U#resolveWorkDirectory(String, String, boolean, boolean)
     * @see U#IGNITE_WORK_DIR
     */
    protected NodeFileTree(IgniteConfiguration cfg, File root, String folderName, boolean isSnapshot) {
        super(root, cfg.getSnapshotPath());

        A.notNull(folderName, "Node directory");

        this.folderName = folderName;

        binaryMeta = new File(binaryMetaRoot, folderName);

        DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

        if (CU.isPersistenceEnabled(cfg) || CU.isCdcEnabled(cfg)) {
            // Snapshots MUST use root relative node storage path.
            nodeStorage = (dsCfg.getStoragePath() == null || isSnapshot)
                ? rootRelative(DB_DIR)
                : resolveDirectory(dsCfg.getStoragePath());
            checkpoint = new File(nodeStorage, CHECKPOINT_DIR);
            wal = resolveDirectory(dsCfg.getWalPath());
            walArchive = resolveDirectory(dsCfg.getWalArchivePath());
            walCdc = resolveDirectory(dsCfg.getCdcWalPath());
        }
        else {
            nodeStorage = rootRelative(DB_DIR);
            checkpoint = new File(nodeStorage, CHECKPOINT_DIR);
            wal = rootRelative(DFLT_WAL_PATH);
            walArchive = rootRelative(DFLT_WAL_ARCHIVE_PATH);
            walCdc = rootRelative(DFLT_WAL_CDC_PATH);
        }

        extraStorages = extraStorages(
            dsCfg,
            storagePath -> resolveDirectory(Path.of(storagePath, DB_DIR).toString())
        );
    }

    /** @return Node storage directory. */
    public File nodeStorage() {
        return nodeStorage;
    }

    /** @return Extra storages. */
    public Map<String, File> extraStorages() {
        return extraStorages;
    }

    /** @return All storages directories. */
    public Stream<File> allStorages() {
        return Stream.concat(Stream.of(nodeStorage), extraStorages.values().stream());
    }

    /** @return Folder name. */
    public String folderName() {
        return folderName;
    }

    /** @return Path to binary metadata directory. */
    public File binaryMeta() {
        return binaryMeta;
    }

    /** @return Path to the directory containing active WAL segments. */
    public @Nullable File wal() {
        return wal;
    }

    /**
     * @param idx Segment number.
     * @return Segment file.
     */
    public File walSegment(long idx) {
        return new File(wal, U.fixedLengthNumberName(idx, WAL_SEGMENT_FILE_EXT));
    }

    /**
     * @param idx Segment number.
     * @return Archive Segment file.
     */
    public File walArchiveSegment(long idx) {
        return new File(walArchive, U.fixedLengthNumberName(idx, WAL_SEGMENT_FILE_EXT));
    }

    /**
     * @param idx Segment number.
     * @return Temp segment file.
     */
    public File tempWalSegment(long idx) {
        return new File(wal, U.fixedLengthNumberName(idx, TMP_WAL_SEG_FILE_EXT));
    }

    /**
     * @param idx Segment number.
     * @return Temp archive Segment file.
     */
    public File tempWalArchiveSegment(long idx) {
        return new File(walArchive, U.fixedLengthNumberName(idx, TMP_WAL_SEG_FILE_EXT));
    }

    /**
     * @param idx Segment number.
     * @return Zipped archive Segment file.
     */
    public File zipWalArchiveSegment(long idx) {
        return new File(walArchive, U.fixedLengthNumberName(idx, ZIP_WAL_SEG_FILE_EXT));
    }

    /**
     * @param idx Segment number.
     * @return Zipped archive Segment file.
     */
    public File zipTempWalArchiveSegment(long idx) {
        return new File(walArchive, U.fixedLengthNumberName(idx, TMP_ZIP_WAL_SEG_FILE_EXT));
    }

    /** @return Path to the directory containing archive WAL segments. */
    public @Nullable File walArchive() {
        return walArchive;
    }

    /** @return Path to the directory containing archive WAL segments for CDC. */
    public @Nullable File walCdc() {
        return walCdc;
    }

    /** @return Path to the directories for temp snapshot files. */
    public List<File> snapshotsTempRoots() {
        return allStorages()
            .map(this::snapshotTempRoot)
            .collect(Collectors.toList());
    }

    /**
     * @param root Root directory.
     * @return Root directory for snapshot temp files.
     */
    protected File snapshotTempRoot(File root) {
        return new File(root, SNAPSHOT_TMP_DIR);
    }

    /** @return Path to the checkpoint directory. */
    public File checkpoint() {
        return checkpoint;
    }

    /**
     * Creates {@link #binaryMeta()} directory.
     * @return Created directory.
     * @see #binaryMeta()
     */
    public File mkdirBinaryMeta() {
        return mkdir(binaryMeta, "binary metadata");
    }

    /**
     * Creates {@link #checkpoint()} directory.
     * @return Created directory.
     * @see #checkpoint()
     */
    public File mkdirCheckpoint() {
        return mkdir(checkpoint, "checkpoint metadata directory");
    }

    /** @return {@code True} if WAL archive enabled. */
    public boolean walArchiveEnabled() {
        return walArchive != null && wal != null && !walArchive.equals(wal);
    }

    /**
     * @param ccfg Cache configuration.
     * @return Store dir for given cache.
     */
    public File cacheStorage(CacheConfiguration<?, ?> ccfg) {
        return new File(cacheStorage(ccfg.getStoragePath()), ccfg.getGroupName() != null
            ? CACHE_GRP_DIR_PREFIX + ccfg.getGroupName()
            : CACHE_DIR_PREFIX + ccfg.getName());
    }

    /**
     * @return All cache directories.
     */
    public List<File> existingCacheDirs() {
        return existingCacheDirs(true, f -> true);
    }

    /**
     * @return Cache directories. Metatorage directory excluded.
     */
    public List<File> existingCacheDirsWithoutMeta() {
        return existingCacheDirs(false, f -> true);
    }

    /**
     * @return Cache directories. Metatorage directory excluded.
     */
    public List<File> existingUserCacheDirs() {
        final String utilityCacheStorage = CACHE_DIR_PREFIX + UTILITY_CACHE_NAME;

        return existingCacheDirs(false, f -> !f.getName().equals(utilityCacheStorage));
    }

    /**
     * @return Temp cache storages.
     */
    public List<File> existingTmpCacheStorages() {
        return filesInStorages(NodeFileTree::isTmpCacheStorage).collect(Collectors.toList());
    }

    /**
     * @param ccfg Cache configuration.
     * @return Cache configuration file with respect to {@link CacheConfiguration#getGroupName} value.
     */
    public File cacheConfigurationFile(CacheConfiguration<?, ?> ccfg) {
        return new File(cacheStorage(ccfg), ccfg.getGroupName() == null
            ? CACHE_DATA_FILENAME
            : (ccfg.getName() + CACHE_DATA_FILENAME));
    }

    /**
     * @param ccfg Cache configuration.
     * @return Cache configuration file with respect to {@link CacheConfiguration#getGroupName} value.
     */
    public File tmpCacheConfigurationFile(CacheConfiguration<?, ?> ccfg) {
        return new File(cacheStorage(ccfg), ccfg.getGroupName() == null
            ? (CACHE_DATA_TMP_FILENAME)
            : (ccfg.getName() + CACHE_DATA_TMP_FILENAME));
    }

    /**
     * @param ccfg Cache configuration.
     * @param part Partition id.
     * @return Partition file.
     */
    public File partitionFile(CacheConfiguration<?, ?> ccfg, int part) {
        return new File(cacheStorage(ccfg), partitionFileName(part));
    }

    /**
     * @param ccfg Cache configuration.
     * @return Store directory for given cache.
     */
    public File tmpCacheStorage(CacheConfiguration<?, ?> ccfg) {
        File cacheStorage = cacheStorage(ccfg);
        return new File(cacheStorage.getParentFile(), TMP_CACHE_DIR_PREFIX + cacheStorage.getName());
    }

    /**
     * Temporary cache storage and partitions are created while snapshot restoring.
     * Moving to regular cache storage when finished.
     *
     * @param storagePath Cache storage path.
     * @param cacheDirName Cache directory name.
     * @return Temp store directory for given cache.
     * @see CacheConfiguration#getStoragePath()
     */
    public File tmpCacheStorage(@Nullable String storagePath, String cacheDirName) {
        return new File(cacheStorage(storagePath), TMP_CACHE_DIR_PREFIX + cacheDirName);
    }

    /**
     * Temporary cache storage and partitions are created while snapshot restoring.
     * Moving to regular cache storage when finished.
     *
     * @param cacheStorage cache storage.
     * @return Temp store directory for given cache storage.
     */
    public File tmpCacheStorage(File cacheStorage) {
        return new File(cacheStorage.getParentFile(), TMP_CACHE_DIR_PREFIX + cacheStorage.getName());
    }

    /**
     * @param part Partition.
     * @return File for metastorage partition.
     */
    public File metaStoragePartition(int part) {
        return new File(metaStorage(), partitionFileName(part));
    }

    /** @return Path to the metastorage directory. */
    public File metaStorage() {
        return new File(nodeStorage, METASTORAGE_DIR_NAME);
    }

    /**
     * Temporary cache partitions are created while snapshot restoring.
     *
     * @param ccfg Cache configuration.
     * @param partId partition id.
     * @return Path to the temp partition file.
     */
    public File tmpPartition(CacheConfiguration<?, ?> ccfg, int partId) {
        return new File(tmpCacheStorage(ccfg), partitionFileName(partId));
    }

    /**
     * Temporary cache partitions are created while snapshot restoring.
     *
     * @param storagePath Cache storage path.
     * @param cacheDirName Cache directory name.
     * @param partId partition id.
     * @return Path to the temp partition file.
     * @see CacheConfiguration#getStoragePath()
     */
    public File tmpPartition(@Nullable String storagePath, String cacheDirName, int partId) {
        return new File(tmpCacheStorage(storagePath, cacheDirName), partitionFileName(partId));
    }

    /** */
    protected static String partitionFileName(int part, String idxName, String format) {
        assert part <= MAX_PARTITION_ID || part == INDEX_PARTITION;

        return part == INDEX_PARTITION ? idxName : format(format, part);
    }

    /**
     * @param dir Directory.
     * @return {@code True} if directory conforms cache storage name pattern.
     * @see #cacheGroupDir(File)
     */
    public static boolean cacheDir(File dir) {
        return dir.getName().startsWith(CACHE_DIR_PREFIX);
    }

    /**
     * @param dir Directory.
     * @return {@code True} if directory conforms cache group storage name pattern.
     */
    private static boolean cacheGroupDir(File dir) {
        return dir.getName().startsWith(CACHE_GRP_DIR_PREFIX);
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms partition file name pattern.
     */
    public static boolean partitionFile(File f) {
        return f.getName().startsWith(PART_FILE_PREFIX);
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms cache(including cache group caches) config file name pattern.
     */
    private static boolean cacheOrCacheGroupConfigFile(File f) {
        return f.getName().endsWith(CACHE_DATA_FILENAME);
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms cache config file name pattern.
     */
    public static boolean cacheConfigFile(File f) {
        return f.getName().equals(CACHE_DATA_FILENAME);
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms cache config file name pattern.
     */
    public static boolean binFile(File f) {
        return f.getName().endsWith(FILE_SUFFIX);
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms temp cache storage name pattern.
     */
    private static boolean isTmpCacheStorage(File f) {
        return f.isDirectory() && f.getName().startsWith(TMP_CACHE_DIR_PREFIX);
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms temp cache configuration file name pattern.
     */
    public static boolean tmpCacheConfig(File f) {
        return f.getName().endsWith(CACHE_DATA_TMP_FILENAME);
    }

    /**
     * @param f File.
     * @return {@code True} if file is regular(not temporary).
     */
    public static boolean notTmpFile(File f) {
        return !f.getName().endsWith(TMP_SUFFIX);
    }

    /**
     * @param f Temporary cache directory.
     * @return Cache or group id.
     */
    public static String tmpDirCacheName(File f) {
        assert isTmpCacheStorage(f) : f;

        return cacheName(f.getName().substring(TMP_CACHE_DIR_PREFIX.length()));
    }

    /**
     * @param f Directory
     * @return Cache name for directory, if it conforms cache storage pattern.
     */
    public static String cacheName(File f) {
        return cacheName(f.getName());
    }

    /**
     * @param root Root directory.
     * @return List of cache data files.
     */
    public static List<File> existingCacheConfigFiles(File root) {
        if (cacheDir(root)) {
            File cfg = new File(root, CACHE_DATA_FILENAME);

            return cfg.exists() ? Collections.singletonList(cfg) : Collections.emptyList();
        }

        return allExisingConfigFiles(root);
    }

    /**
     * @param root Root directory.
     * @return List of cache data files regardless directory name.
     */
    public static List<File> allExisingConfigFiles(File root) {
        return F.asList(root.listFiles(NodeFileTree::cacheOrCacheGroupConfigFile));
    }

    /**
     * @param part Partition id.
     * @return File name.
     */
    public static String partitionFileName(int part) {
        return partitionFileName(part, INDEX_FILE_NAME, PART_FILE_TEMPLATE);
    }

    /**
     * @param name File name.
     * @return Cache name.
     */
    private static String cacheName(String name) {
        if (name.startsWith(CACHE_GRP_DIR_PREFIX))
            return name.substring(CACHE_GRP_DIR_PREFIX.length());
        else if (name.startsWith(CACHE_DIR_PREFIX))
            return name.substring(CACHE_DIR_PREFIX.length());
        else if (name.equals(METASTORAGE_DIR_NAME))
            return METASTORAGE_CACHE_NAME;
        else
            throw new IgniteException("Directory doesn't match the cache or cache group prefix: " + name);
    }

    /**
     * @param storagePath Value from config.
     * @return File storage.
     * @see CacheConfiguration#getStoragePath()
     */
    private File cacheStorage(@Nullable String storagePath) {
        return storagePath == null ? nodeStorage : extraStorages.getOrDefault(storagePath, nodeStorage);
    }

    /**
     * Key is storage path from config, may be relative.
     * Value is actual storage path.
     *
     * @param dsCfg Data storage configuration.
     * @return Node storages.
     * @see DataStorageConfiguration#setExtraStoragePathes(String...)
     */
    private Map<String, File> extraStorages(@Nullable DataStorageConfiguration dsCfg, Function<String, File> resolver) {
        if (dsCfg == null || F.isEmpty(dsCfg.getExtraStoragePathes()))
            return Collections.emptyMap();

        return Arrays.stream(dsCfg.getExtraStoragePathes())
            .collect(Collectors.toMap(Function.identity(), resolver));
    }

    /**
     * @param segment WAL segment file.
     * @return Segment index.
     */
    public long walSegmentIndex(Path segment) {
        String fn = segment.getFileName().toString();

        return Long.parseLong(fn.substring(0, fn.indexOf('.')));
    }

    /**
     * @return Tree for metastorage cache.
     */
    public CacheFileTree metastoreTree() {
        return new CacheFileTree(this, true, null);
    }

    /**
     * @param ccfg Cache configuration.
     * @return Tree for cache.
     */
    public CacheFileTree cacheTree(CacheConfiguration<?, ?> ccfg) {
        return new CacheFileTree(this, false, ccfg);
    }

    /**
     * @param includeMeta If {@code true} then include metadata directory into results.
     * @param filter Cache group names to filter.
     * @return Cache directories that matches filters criteria.
     */
    protected List<File> existingCacheDirs(boolean includeMeta, Predicate<File> filter) {
        Predicate<File> dirFilter = includeMeta ? CACHE_DIR_WITH_META_FILTER : CACHE_DIR_FILTER;

        return filesInStorages(f -> f.isDirectory() && dirFilter.test(f) && filter.test(f)).collect(Collectors.toList());
    }

    /**
     * @param part Partition file name.
     * @return Partition id.
     */
    public static int partId(File part) {
        String name = part.getName();
        if (name.equals(INDEX_FILE_NAME))
            return INDEX_PARTITION;

        if (name.startsWith(PART_FILE_PREFIX))
            return Integer.parseInt(name.substring(PART_FILE_PREFIX.length(), name.indexOf('.')));

        throw new IllegalStateException("Illegal partition file name: " + name);
    }

    /**
     * @param filter Dir file filter.
     * @return All files from all {@link #extraStorages} and {@link #nodeStorage} matching the filter.
     */
    private Stream<File> filesInStorages(FileFilter filter) {
        return allStorages().flatMap(storage -> {
            File[] storageFiles = storage.listFiles(filter);

            return storageFiles == null
                ? Stream.empty()
                : Arrays.stream(storageFiles);
        });
    }

    /**
     * Resolves directory specified by the given arguments.
     *
     * @param cfg Configured directory path.
     * @return Initialized directory.
     */
    private File resolveDirectory(String cfg) {
        File sharedDir = new File(cfg);

        return sharedDir.isAbsolute()
            ? new File(sharedDir, folderName)
            : rootRelative(cfg);
    }

    /** @return {@code ${root}/${path}/${folderName}} path. */
    private File rootRelative(String path) {
        return Paths.get(root.getAbsolutePath(), path, folderName).toFile();
    }

    /**
     * @param typeId Type id.
     * @return Binary metadata file name.
     */
    public static String binaryMetaFileName(int typeId) {
        return typeId + FILE_SUFFIX;
    }

    /**
     * @param fileName File name.
     * @return Type id
     * @see #binaryMetaFileName(int)
     * @see NodeFileTree#FILE_SUFFIX
     */
    public static int typeId(String fileName) {
        return Integer.parseInt(fileName.substring(0, fileName.length() - FILE_SUFFIX.length()));
    }

    /** @return CDC state directory path. */
    public Path cdcState() {
        return walCdc().toPath().resolve(STATE_DIR);
    }

    /** @return CDC WAL state file path. */
    public Path cdcWalState() {
        return cdcState().resolve(WAL_STATE_FILE_NAME);
    }

    /** @return Temp CDC WAL state file path. */
    public Path tmpCdcWalState() {
        return cdcState().resolve(WAL_STATE_FILE_NAME + TMP_SUFFIX);
    }

    /** @return CDC types state file path. */
    public Path cdcTypesState() {
        return cdcState().resolve(TYPES_STATE_FILE_NAME);
    }

    /** @return Temp CDC types state file path. */
    public Path tmpCdcTypesState() {
        return cdcState().resolve(TYPES_STATE_FILE_NAME + TMP_SUFFIX);
    }

    /** @return CDC mappings state file path. */
    public Path cdcMappingsState() {
        return cdcState().resolve(MAPPINGS_STATE_FILE_NAME);
    }

    /** @return Temp CDC mappings state file path. */
    public Path tmpCdcMappingsState() {
        return cdcState().resolve(MAPPINGS_STATE_FILE_NAME + TMP_SUFFIX);
    }

    /** @return CDC caches state file path. */
    public Path cdcCachesState() {
        return cdcState().resolve(CACHES_STATE_FILE_NAME);
    }

    /** @return Temp CDC caches state file path. */
    public Path tmpCdcCachesState() {
        return cdcState().resolve(CACHES_STATE_FILE_NAME + TMP_SUFFIX);
    }

    /** @return CDC manager mode state file path. */
    public Path cdcModeState() {
        return cdcState().resolve(CDC_MODE_FILE_NAME);
    }

    /** @return Temp CDC manager mode state file path. */
    public Path tmpCdcModeState() {
        return cdcState().resolve(CDC_MODE_FILE_NAME + TMP_SUFFIX);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NodeFileTree.class, this);
    }
}
