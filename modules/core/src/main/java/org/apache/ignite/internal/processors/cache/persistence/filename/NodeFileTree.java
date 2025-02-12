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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Predicate;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_CDC_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.DB_DEFAULT_FOLDER;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_DIR_NAME;

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
 * │  │  │  ├── index.bin
 * │  │  │  ├── part-0.bin
 * │  │  │  ├── part-1.bin
 * ...
 * │  │  │  └── part-9.bin
 * │  │  ├── cache-ignite-sys-cache                                             ← cacheStorage (cache name "ignite-sys-cache").
 * │  │  │  ├── cache_data.dat                                                  ← cache("ignite-sys-cache") configuration file.
 * │  │  │  └── index.bin
 * │  │  ├── cacheGroup-tx-cache                                                ← cacheStorage (cache group "tx-cache").
 * │  │  │  ├── tx-cachecache_data.dat                                          ← cache("tx-cache") configuration file inside group.
 * │  │  │  ├── othercache_data.dat                                             ← cache("other") configuration file inside group.
 * │  │  │  ├── index.bin
 * │  │  │  ├── part-0.bin
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
    public static final String SNAPSHOT_TMP_DIR = "snp";

    /** Checkpoint directory name. */
    public static final String CHECKPOINT_DIR = "cp";

    /** File extension of WAL segment. */
    public static final String WAL_SEGMENT_FILE_EXT = ".wal";

    /** File suffix. */
    public static final String FILE_SUFFIX = ".bin";

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
    public static final Predicate<File> CACHE_DIR_FILTER = dir -> cacheDir(dir) || cacheGroupDir(dir);

    /** Prefix for {@link #cacheStorage(String)} directory in case of single cache. */
    private static final String CACHE_DIR_PREFIX = "cache-";

    /** Prefix for {@link #cacheStorage(String)} directory in case of cache group. */
    private static final String CACHE_GRP_DIR_PREFIX = "cacheGroup-";

    /** Filter out all cache directories including {@link MetaStorage}. */
    public static final Predicate<File> CACHE_DIR_WITH_META_FILTER = dir ->
        CACHE_DIR_FILTER.test(dir) ||
            dir.getName().equals(MetaStorage.METASTORAGE_DIR_NAME);

    /** Partition file prefix. */
    public static final String PART_FILE_PREFIX = "part-";

    /** Index file prefix. */
    public static final String INDEX_FILE_PREFIX = "index";

    /** Index file name. */
    public static final String INDEX_FILE_NAME = INDEX_FILE_PREFIX + FILE_SUFFIX;

    /** Partition file template. */
    public static final String PART_FILE_TEMPLATE = PART_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** */
    public static final String CACHE_DATA_FILENAME = "cache_data.dat";

    /** */
    public static final String CACHE_DATA_TMP_FILENAME = CACHE_DATA_FILENAME + TMP_SUFFIX;

    /** Folder name for consistent id. */
    private final String folderName;

    /** Path to the directory containing binary metadata. */
    private final File binaryMeta;

    /** Path to the storage directory. */
    private final @Nullable File nodeStorage;

    /** Path to the checkpoint directory. */
    private final @Nullable File checkpoint;

    /** Path to the directory containing active WAL segments. */
    private final @Nullable File wal;

    /** Path to the directory containing archive WAL segments. */
    private final @Nullable File walArchive;

    /** Path to the directory containing archive WAL segments for CDC. */
    private final @Nullable File walCdc;

    /**
     * Working directory for loaded snapshots from the remote nodes and storing
     * temporary partition delta-files of locally started snapshot process.
     */
    private final @Nullable File snpTmpRoot;

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
    public NodeFileTree(String root, String folderName) {
        this(new File(root), folderName);
    }

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
        snpTmpRoot = new File(nodeStorage, SNAPSHOT_TMP_DIR);
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
        super(cfg);

        A.notNull(folderName, "Node directory");

        this.folderName = folderName;

        binaryMeta = new File(binaryMetaRoot, folderName);

        DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

        if (CU.isPersistenceEnabled(cfg) || CU.isCdcEnabled(cfg)) {
            nodeStorage = dsCfg.getStoragePath() == null
                ? rootRelative(DB_DEFAULT_FOLDER)
                : resolveDirectory(dsCfg.getStoragePath());
            snpTmpRoot = new File(nodeStorage, SNAPSHOT_TMP_DIR);
            checkpoint = new File(nodeStorage, CHECKPOINT_DIR);
            wal = resolveDirectory(dsCfg.getWalPath());
            walArchive = resolveDirectory(dsCfg.getWalArchivePath());
            walCdc = resolveDirectory(dsCfg.getCdcWalPath());
        }
        else {
            nodeStorage = null;
            snpTmpRoot = null;
            checkpoint = null;
            wal = null;
            walArchive = null;
            walCdc = null;
        }
    }

    /** @return Node storage directory. */
    public File nodeStorage() {
        return nodeStorage;
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

    /** @return Path to the directory form temp snapshot files. */
    public @Nullable File snapshotTempRoot() {
        return snpTmpRoot;
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
     * Creates {@link #snapshotTempRoot()} directory.
     * @return Created directory.
     * @see #snapshotTempRoot()
     */
    public File mkdirSnapshotTempRoot() {
        return mkdir(snpTmpRoot, "temp directory for snapshot creation");
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
        return cacheStorage(cacheDirName(ccfg));
    }

    /**
     * @param isSharedGroup {@code True} if cache is sharing the same `underlying` cache.
     * @param cacheOrGroupName Cache name.
     * @return The full cache directory name.
     */
    public File cacheStorage(boolean isSharedGroup, String cacheOrGroupName) {
        return cacheStorage(cacheDirName(isSharedGroup, cacheOrGroupName));
    }

    /**
     * @param ccfg Cache configuration.
     * @return The full cache directory name.
     */
    public String cacheDirName(CacheConfiguration<?, ?> ccfg) {
        boolean isSharedGrp = ccfg.getGroupName() != null;

        return cacheDirName(isSharedGrp, CU.cacheOrGroupName(ccfg));
    }

    /**
     * @param ccfg Cache configuration.
     * @return Cache configuration file with respect to {@link CacheConfiguration#getGroupName} value.
     */
    public File cacheConfigurationFile(CacheConfiguration<?, ?> ccfg) {
        return new File(cacheStorage(ccfg), cacheDataFilename(ccfg));
    }

    /** @return Name of cache data filename. */
    public static String cacheDataFilename(CacheConfiguration<?, ?> ccfg) {
        return ccfg.getGroupName() == null ? CACHE_DATA_FILENAME : (ccfg.getName() + CACHE_DATA_FILENAME);
    }

    /**
     * @param cacheDirName Cache directory name.
     * @param part Partition id.
     * @return Partition file.
     */
    public File partitionFile(String cacheDirName, int part) {
        return new File(cacheStorage(cacheDirName), partitionFileName(part));
    }

    /**
     * @param workDir Cache work directory.
     * @param cacheDirName Cache directory name.
     * @param part Partition id.
     * @return Partition file.
     */
    @NotNull public static File partitionFile(File workDir, String cacheDirName, int part) {
        return new File(cacheStorage(workDir, cacheDirName), partitionFileName(part));
    }

    /**
     * @param part Partition id.
     * @return File name.
     */
    public static String partitionFileName(int part) {
        assert part <= MAX_PARTITION_ID || part == INDEX_PARTITION;

        return part == INDEX_PARTITION ? INDEX_FILE_NAME : format(PART_FILE_TEMPLATE, part);
    }

    /**
     * @param cacheDirName Cache directory name.
     * @return Store directory for given cache.
     */
    public static File cacheStorage(File storeWorkDir, String cacheDirName) {
        return new File(storeWorkDir, cacheDirName);
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
    public static boolean cacheGroupDir(File dir) {
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
    public static boolean cacheOrCacheGroupConfigFile(File f) {
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
     * @param cacheDirName Cache directory name.
     * @return Store directory for given cache.
     */
    public File cacheStorage(String cacheDirName) {
        return new File(nodeStorage, cacheDirName);
    }

    /**
     * @param isSharedGroup {@code True} if cache is sharing the same `underlying` cache.
     * @param cacheOrGroupName Cache name.
     * @return The full cache directory name.
     */
    public static String cacheDirName(boolean isSharedGroup, String cacheOrGroupName) {
        if (cacheOrGroupName.equals(METASTORAGE_CACHE_NAME))
            return METASTORAGE_DIR_NAME;

        return isSharedGroup
            ? CACHE_GRP_DIR_PREFIX + cacheOrGroupName
            : CACHE_DIR_PREFIX + cacheOrGroupName;
    }

    /**
     * @param dir Directory
     * @return Cache name for directory, if it conforms cache storage pattern.
     */
    public static String cacheName(File dir) {
        String name = dir.getName();

        if (name.startsWith(CACHE_GRP_DIR_PREFIX))
            return name.substring(CACHE_GRP_DIR_PREFIX.length());
        else if (name.startsWith(CACHE_DIR_PREFIX))
            return name.substring(CACHE_DIR_PREFIX.length());
        else if (name.equals(MetaStorage.METASTORAGE_DIR_NAME))
            return METASTORAGE_CACHE_NAME;
        else
            throw new IgniteException("Directory doesn't match the cache or cache group prefix: " + dir);
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NodeFileTree.class, this);
    }
}
