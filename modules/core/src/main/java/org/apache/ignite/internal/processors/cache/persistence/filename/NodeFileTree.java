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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
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
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.DB_DEFAULT_FOLDER;
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
    public static final String FILE_SUFFIX = ".bin";

    /** Suffix for tmp files */
    public static final String TMP_SUFFIX = ".tmp";

    /** Suffix for zip files */
    public static final String ZIP_SUFFIX = ".zip";

    /** File extension of temp WAL segment. */
    static final String TMP_WAL_SEG_FILE_EXT = WAL_SEGMENT_FILE_EXT + TMP_SUFFIX;

    /** File extension of zipped WAL segment. */
    public static final String ZIP_WAL_SEG_FILE_EXT = WAL_SEGMENT_FILE_EXT + ZIP_SUFFIX;

    /** File extension of temp zipped WAL segment. */
    public static final String TMP_ZIP_WAL_SEG_FILE_EXT = ZIP_WAL_SEG_FILE_EXT + TMP_SUFFIX;

    /** Filter out all cache directories. */
    private static final Predicate<File> CACHE_DIR_FILTER = dir -> FileTreeUtils.cacheDir(dir)
        || dir.getName().startsWith(NodeFileTree.CACHE_GRP_DIR_PREFIX);

    /** Filter out all cache directories including {@link MetaStorage}. */
    private static final Predicate<File> CACHE_DIR_WITH_META_FILTER = dir ->
        CACHE_DIR_FILTER.test(dir) ||
            dir.getName().equals(METASTORAGE_DIR_NAME);

    /** Partition file prefix. */
    public static final String PART_FILE_PREFIX = "part-";

    /** Index file prefix. */
    public static final String INDEX_FILE_PREFIX = "index";

    /** Index file name. */
    protected static final String INDEX_FILE_NAME = INDEX_FILE_PREFIX + FILE_SUFFIX;

    /** Partition file template. */
    protected static final String PART_FILE_TEMPLATE = PART_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** */
    static final String CACHE_DATA_FILENAME = "cache_data.dat";

    /** */
    static final String CACHE_DATA_TMP_FILENAME = CACHE_DATA_FILENAME + TMP_SUFFIX;

    /** Temporary cache directory prefix. */
    static final String TMP_CACHE_DIR_PREFIX = "_tmp_snp_restore_";

    /**
     * Prefix for {@link #cacheStorage(CacheConfiguration)} directory in case of single cache.
     * {@link CacheConfiguration#getGroupName()} is null.
     */
    static final String CACHE_DIR_PREFIX = "cache-";

    /**
     * Prefix for {@link #cacheStorage(CacheConfiguration)} directory in case of cache group.
     * {@link CacheConfiguration#getGroupName()} is not null.
     */
    static final String CACHE_GRP_DIR_PREFIX = "cacheGroup-";

    /** Folder name for consistent id. */
    private final String folderName;

    /** Path to the directory containing binary metadata. */
    private final File binaryMeta;

    /** Path to the storage directory. */
    private final File nodeStorage;

    /**
     * Key is the name of data region({@link DataRegionConfiguration#getName()}), value is node storage for this data region.
     * @see DataRegionConfiguration#setStoragePath(String)
     */
    protected final Map<String, File> drStorages;

    /**
     * Name of the default data region.
     * @see DataStorageConfiguration#DFLT_DATA_REG_DEFAULT_NAME
     * @see DataStorageConfiguration#setDefaultDataRegionConfiguration(DataRegionConfiguration)
     * @see DataRegionConfiguration#setName(String)
     */
    private final String dfltDrName;

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
        wal = rootRelative(DFLT_WAL_PATH);
        walArchive = rootRelative(DFLT_WAL_ARCHIVE_PATH);
        walCdc = rootRelative(DFLT_WAL_CDC_PATH);
        nodeStorage = rootRelative(DB_DEFAULT_FOLDER);
        drStorages = Collections.emptyMap();
        dfltDrName = DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
        checkpoint = new File(nodeStorage, CHECKPOINT_DIR);
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
        this(cfg, root(cfg), folderName);
    }

    /**
     * Creates instance based on config and folder name.
     *
     * @param root Root directory.
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
    protected NodeFileTree(IgniteConfiguration cfg, File root, String folderName) {
        super(root, cfg.getSnapshotPath());

        A.notNull(folderName, "Node directory");

        this.folderName = folderName;

        binaryMeta = new File(binaryMetaRoot, folderName);

        DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

        if (CU.isPersistenceEnabled(cfg) || CU.isCdcEnabled(cfg)) {
            nodeStorage = dsCfg.getStoragePath() == null
                ? rootRelative(DB_DEFAULT_FOLDER)
                : resolveDirectory(dsCfg.getStoragePath());

            dfltDrName = dsCfg.getDefaultDataRegionConfiguration().getName();
            checkpoint = new File(nodeStorage, CHECKPOINT_DIR);
            wal = resolveDirectory(dsCfg.getWalPath());
            walArchive = resolveDirectory(dsCfg.getWalArchivePath());
            walCdc = resolveDirectory(dsCfg.getCdcWalPath());
        }
        else {
            nodeStorage = rootRelative(DB_DEFAULT_FOLDER);
            dfltDrName = DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
            checkpoint = new File(nodeStorage, CHECKPOINT_DIR);
            wal = rootRelative(DFLT_WAL_PATH);
            walArchive = rootRelative(DFLT_WAL_ARCHIVE_PATH);
            walCdc = rootRelative(DFLT_WAL_CDC_PATH);
        }

        drStorages = dataRegionStorages(
            dsCfg,
            (drName, drStoragePath) -> resolveDirectory(Path.of(drStoragePath, DB_DEFAULT_FOLDER).toString())
        );
    }

    /** @return Node storage directory. */
    public File nodeStorage() {
        return nodeStorage;
    }

    /** @return Storages for data regions. */
    public Map<String, File> dataRegionStorages() {
        return drStorages;
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
     * @return Zip entry name.
     */
    public static String zipWalEntryName(long idx) {
        return idx + WAL_SEGMENT_FILE_EXT;
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

    /** @return Path to the directory form temp snapshot files. */
    protected File snapshotTempRoot() {
        return snapshotTempRoot(nodeStorage);
    }

    /** @return Path to the directory form temp snapshot files. */
    public List<File> snapshotsTempRoots() {
        return Stream.concat(Stream.of(nodeStorage), drStorages.values().stream())
            .map(this::snapshotTempRoot)
            .collect(Collectors.toList());
    }

    /** */
    private File snapshotTempRoot(File root) {
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
        return new File(
            dataRegionStorage(ccfg.getDataRegionName()),
            cacheDirName(ccfg)
        );
    }

    /**
     * @param ccfg Cache configuration.
     * @return Store directory for given cache.
     */
    public File tmpCacheStorage(CacheConfiguration<?, ?> ccfg) {
        return new File(dataRegionStorage(ccfg.getDataRegionName()), TMP_CACHE_DIR_PREFIX + cacheDirName(ccfg));
    }

    /**
     * @param cacheStorage cache storage.
     * @return Store directory for given cache.
     */
    public File tmpCacheStorage(File cacheStorage) {
        return new File(cacheStorage.getParentFile(), TMP_CACHE_DIR_PREFIX + cacheStorage.getName());
    }

    /**
     * @param drName Data region name.
     * @param cacheDirName Cache directory name.
     * @return Store directory for given cache.
     */
    public File tmpCacheStorage(String drName, String cacheDirName) {
        return new File(dataRegionStorage(drName), TMP_CACHE_DIR_PREFIX + cacheDirName);
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
     * @param ccfg Cache configuration.
     * @param partId partition id.
     * @return Path to the temp partition file.
     */
    public File tmpPartition(CacheConfiguration<?, ?> ccfg, int partId) {
        return new File(tmpCacheStorage(ccfg), partitionFileName(partId));
    }

    /**
     * @param drName Data region name.
     * @param cacheDirName Cache directory name.
     * @param partId partition id.
     * @return Path to the temp partition file.
     */
    public File tmpPartition(String drName, String cacheDirName, int partId) {
        return new File(tmpCacheStorage(drName, cacheDirName), partitionFileName(partId));
    }

    /**
     * @param segment WAL segment file.
     * @return Segment index.
     */
    public long walSegmentIndex(Path segment) {
        String fn = segment.getFileName().toString();

        return Long.parseLong(fn.substring(0, fn.indexOf('.')));
    }

    /** @return All cache directories. */
    public List<File> existingCacheDirs() {
        return existingCacheDirs(true, f -> true);
    }

    /** @return Cache directories. Metatorage directory excluded. */
    public List<File> existingCacheDirsWithoutMeta() {
        return existingCacheDirs(false, f -> true);
    }

    /** @return Temp cache storages. */
    public List<File> existingTmpCacheStorages() {
        return filesInStorages(FileTreeUtils::tmpCacheStorage).collect(Collectors.toList());
    }

    /** @return Cache directories. Metatorage directory excluded. */
    public List<File> existingUserCacheDirs() {
        final String utilityCacheStorage = CACHE_DIR_PREFIX + UTILITY_CACHE_NAME;

        return existingCacheDirs(false, f -> !f.getName().equals(utilityCacheStorage));
    }

    /**
     * @param f Temporary cache directory.
     * @return Cache or group id.
     */
    public static String tmpDirCacheName(File f) {
        assert FileTreeUtils.tmpCacheStorage(f) : f;

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
     * @return Array of cache data files.
     */
    public static List<File> existingCacheConfigFiles(File root) {
        if (FileTreeUtils.cacheDir(root)) {
            File cfg = new File(root, CACHE_DATA_FILENAME);

            return cfg.exists() ? Collections.singletonList(cfg) : Collections.emptyList();
        }

        return F.asList(root.listFiles(f -> f.getName().endsWith(CACHE_DATA_FILENAME)));
    }

    /**
     * @param part Partition id.
     * @return File name.
     */
    public static String partitionFileName(int part) {
        return partitionFileName(part, INDEX_FILE_NAME, PART_FILE_TEMPLATE);
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

    /** */
    protected static String partitionFileName(int part, String idxName, String format) {
        assert part <= MAX_PARTITION_ID || part == INDEX_PARTITION;

        return part == INDEX_PARTITION ? idxName : format(format, part);
    }

    /**
     * @param ccfg Cache configurtion.
     * @return The full cache directory name.
     */
    static String cacheDirName(CacheConfiguration<?, ?> ccfg) {
        return (ccfg.getGroupName() == null ? CACHE_DIR_PREFIX : CACHE_GRP_DIR_PREFIX) + CU.cacheOrGroupName(ccfg);
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
     * @param drName Data region name.
     * @return Data region storage.
     */
    private File dataRegionStorage(String drName) {
        return drStorages.getOrDefault(drName == null ? dfltDrName : drName, nodeStorage);
    }

    /**
     * Key is data region name.
     * Value is data region storage.
     * 
     * @param dsCfg Data storage configuration.
     * @return Data regions storages.
     * @see DataRegionConfiguration#setStoragePath(String) 
     */
    protected Map<String, File> dataRegionStorages(@Nullable DataStorageConfiguration dsCfg, BiFunction<String, String, File> resolver) {
        if (dsCfg == null)
            return Collections.emptyMap();

        Map<String, File> customDsStorages = new HashMap<>();

        if (dsCfg.getDataRegionConfigurations() != null) {
            for (DataRegionConfiguration drCfg : dsCfg.getDataRegionConfigurations()) {
                if (drCfg.getStoragePath() == null)
                    continue;

                customDsStorages.put(drCfg.getName(), resolver.apply(drCfg.getName(), drCfg.getStoragePath()));
            }
        }

        DataRegionConfiguration dfltDr = dsCfg.getDefaultDataRegionConfiguration();

        if (dfltDr.getStoragePath() != null)
            customDsStorages.put(dfltDr.getName(), resolver.apply(dfltDr.getName(), dfltDr.getStoragePath()));

        return customDsStorages;
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
     * @param filter Dir file filter.
     * @return All files from all {@link #drStorages} matching the filter.
     */
    private Stream<File> filesInStorages(FileFilter filter) {
        return Stream.concat(Stream.of(nodeStorage), drStorages.values().stream()).flatMap(drStorage -> {
            File[] drStorageFiles = drStorage.listFiles(filter);

            return drStorageFiles == null
                ? Stream.empty()
                : Arrays.stream(drStorageFiles);
        });
    }

    /**
     * Resolves directory specified by the given arguments.
     *
     * @param cfg Configured directory path.
     * @return Initialized directory.
     */
    protected File resolveDirectory(String cfg) {
        File sharedDir = new File(cfg);

        return sharedDir.isAbsolute()
            ? new File(sharedDir, folderName)
            : rootRelative(cfg);
    }

    /** @return {@code ${root}/${path}/${folderName}} path. */
    private File rootRelative(String path) {
        return Paths.get(root.getAbsolutePath(), path, folderName).toFile();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NodeFileTree.class, this);
    }
}
