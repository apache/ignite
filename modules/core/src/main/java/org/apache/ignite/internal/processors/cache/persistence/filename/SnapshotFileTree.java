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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpReader;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * {@link NodeFileTree} extension with the methods required to work with snapshot file tree.
 * During creation, full snapshot, creates the same file tree as regular node.
 * But, using snapshot directory as root.
 */
public class SnapshotFileTree extends NodeFileTree {
    /** File with delta pages suffix. */
    private static final String DELTA_SUFFIX = ".delta";

    /** File with delta pages index suffix. */
    private static final String DELTA_IDX_SUFFIX = ".idx";

    /** Snapshot metafile extension. */
    private static final String SNAPSHOT_METAFILE_EXT = ".smf";

    /** File name template consists of delta pages. */
    private static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** File name template for index delta pages. */
    private static final String INDEX_DELTA_NAME = INDEX_FILE_NAME + DELTA_SUFFIX;

    /** Lock file for dump directory. */
    private static final String DUMP_LOCK = "dump.lock";

    /** Incremental snapshots directory name. */
    private static final String INC_SNP_DIR = "increments";

    /** Pattern for incremental snapshot directory names. */
    private static final Pattern INC_SNP_NAME_PATTERN = U.fixedLengthNumberNamePattern(null);

    /** Dump files name. */
    private static final String DUMP_FILE_EXT = ".dump";

    /** Snapshot name. */
    private final String name;

    /** Snapshot path. */
    @Nullable private final String path;

    /**
     * Consistent id for snapshot.
     * Nullable, only for {@link DumpReader} when we don't know specific consistenId.
     */
    @Nullable private final String consId;

    /** Node file tree relative to {@link #tempFileTree()}. */
    private NodeFileTree tmpFt;

    /**
     * @param ctx Kernal context.
     * @param name Snapshot name.
     * @param path Optional snapshot path.
     */
    public SnapshotFileTree(GridKernalContext ctx, String name, @Nullable String path) {
        this(ctx, name, path, ctx.pdsFolderResolver().fileTree().folderName(), ctx.discovery().localNode().consistentId().toString());
    }

    /**
     * @param ctx Kernal context.
     * @param consId Consistent id.
     * @param name Snapshot name.
     * @param path Optional snapshot path.
     */
    public SnapshotFileTree(GridKernalContext ctx, String name, @Nullable String path, String folderName, String consId) {
        this(ctx.config(), ctx.pdsFolderResolver().fileTree(), name, path, folderName, consId);

        A.notNull(consId, "consistent id");

        this.tmpFt = tempFileTree(ctx);
    }

    /**
     * @param cfg Ignite configuration.
     * @param ft Node file tree.
     * @param name Snapshot name.
     * @param path Snapshot path.
     * @param folderName Folder name.
     * @param consId Consistent id.
     */
    public SnapshotFileTree(
        IgniteConfiguration cfg,
        NodeFileTree ft,
        String name,
        @Nullable String path,
        String folderName,
        @Nullable String consId
    ) {
        super(cfg, root(ft, name, path), folderName);

        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");

        this.name = name;
        this.path = path;
        this.consId = consId;

        Map<String, File> snpDrStorages = snapshotDataRegionStorages(ft, cfg.getSnapshotPath());

        if (snpDrStorages.isEmpty())
            drStorages.clear();
        else
            drStorages.putAll(snpDrStorages);

        this.tmpFt = null;
    }

    /** @return Snapshot name. */
    public String name() {
        return name;
    }

    /** @return Snapshot path. */
    @Nullable public String path() {
        return path;
    }

    /** @return Snapshot temp file tree. */
    public NodeFileTree tempFileTree() {
        return tmpFt;
    }

    /**
     * Returns file tree for specific incremental snapshot.
     * Root will be something like {@code "work/snapshots/mybackup/increments/0000000000000001"}.
     *
     * @param incIdx Increment index.
     * @return Incremental snapshot file tree.
     */
    public IncrementalSnapshotFileTree incrementalSnapshotFileTree(int incIdx) {
        return new IncrementalSnapshotFileTree(
            new File(incrementsRoot(), U.fixedLengthNumberName(incIdx, null)),
            U.maskForFileName(folderName()),
            incIdx
        );
    }

    /**
     * @param ccfg Cache configuration.
     * @param part Partition.
     * @return Cache partition delta file.
     */
    public File partDeltaFile(CacheConfiguration<?, ?> ccfg, int part) {
        return new File(tmpFt.cacheStorage(ccfg), partitionFileName(part, INDEX_DELTA_NAME, PART_DELTA_TEMPLATE));
    }

    /**
     * @param part Partition.
     * @return Metastorage partition delta file.
     */
    public File metastorageDeltaFile(int part) {
        return new File(tmpFt.metaStorage(), partitionFileName(part, INDEX_DELTA_NAME, PART_DELTA_TEMPLATE));
    }

    /**
     * @return Dump lock file.
     */
    public File dumpLock() {
        return new File(nodeStorage(), DUMP_LOCK);
    }

    /**
     * Returns root folder for incremental snapshot.
     * For example, {@code "work/snapshots/mybackup/increments/"}.
     *
     * @return Local snapshot directory where snapshot files are located.
     */
    public File incrementsRoot() {
        return new File(root(), INC_SNP_DIR);
    }

    /**
     * @return Snapshot metadata file.
     */
    public File meta() {
        return new File(root, snapshotMetaFileName(consId));
    }

    /**
     * @return Temp snapshot metadata file.
     */
    public File tmpMeta() {
        return new File(root, snapshotMetaFileName(consId) + TMP_SUFFIX);
    }

    /**
     * Note, this consistent id can differ from the local consistent id.
     * In case snapshot was moved from other node.
     * @return Consistent id of the snapshot.
     */
    public String consistentId() {
        return consId;
    }

    /**
     * @param ccfg Cache configuration.
     * @param part partition.
     * @param compress {@code True} if dump compressed.
     * @return Path to the dump partition file;
     */
    public File dumpPartition(CacheConfiguration<?, ?> ccfg, int part, boolean compress) {
        return new File(cacheStorage(ccfg), dumpPartFileName(part, compress));
    }

    /**
     * @param grpId Cache group id.
     * @return Files that match cache or cache group pattern.
     */
    public File existingCacheDirectory(int grpId) {
        return F.first(existingCacheDirs(true, f -> CU.cacheId(cacheName(f)) == grpId));
    }

    /**
     * @param cacheDir Cache directory to check.
     * @param dump If {@code true} then list dump files.
     * @param compress If {@code true} then list compressed files.
     * @return List of cache partitions in given directory.
     */
    public List<File> existingCachePartitionFiles(File cacheDir, boolean dump, boolean compress) {
        File[] files = cacheDir.listFiles(f -> f.isFile() && f.getName().endsWith(partExtension(dump, compress)));

        return files == null
            ? Collections.emptyList()
            : Arrays.asList(files);
    }

    /**
     * @param part Partition number.
     * @param compressed If {@code true} then compressed partition file.
     * @return Dump partition file name.
     */
    public static String dumpPartFileName(int part, boolean compressed) {
        return PART_FILE_PREFIX + part + partExtension(true, compressed);
    }

    /**
     * @param dump Extension for dump files.
     * @param compressed If {@code true} then files compressed.
     * @return Partition file extension.
     */
    private static String partExtension(boolean dump, boolean compressed) {
        return (dump ? DUMP_FILE_EXT : FILE_SUFFIX) + (compressed ? ZIP_SUFFIX : "");

    }

    /**
     * @param f File.
     * @return {@code True} if file conforms partition dump file name pattern.
     */
    public static boolean dumpPartitionFile(File f, boolean compressed) {
        return partitionFile(f) && f.getName().endsWith(partExtension(true, compressed));
    }

    /**
     * @param f File.
     * @return {@code True} if file conforms snapshot meta name pattern.
     */
    public static boolean snapshotMetaFile(File f) {
        return f.getName().toLowerCase().endsWith(SNAPSHOT_METAFILE_EXT);
    }

    /**
     * @param snpDir Directory to check.
     * @return {@code True} if directory conforms increment snapshot directory pattern.
     */
    public static boolean incrementSnapshotDir(File snpDir) {
        return INC_SNP_NAME_PATTERN.matcher(snpDir.getName()).matches() && snpDir.getAbsolutePath().contains(INC_SNP_DIR);
    }

    /**
     * Partition delta index file. Represents a sequence of page indexes that written to a delta.
     *
     * @param delta File with delta pages.
     * @return File with delta pages index.
     */
    public static File partDeltaIndexFile(File delta) {
        return new File(delta.getParent(), delta.getName() + DELTA_IDX_SUFFIX);
    }

    /**
     * @param consId Consistent node id.
     * @return Snapshot metadata file name.
     */
    public static String snapshotMetaFileName(String consId) {
        return U.maskForFileName(consId) + SNAPSHOT_METAFILE_EXT;
    }

    /**
     * @param ft Node file tree.
     * @param name Snapshot name.
     * @param path Optional snapshot path.
     * @return Path to the snapshot root directory.
     */
    public static File root(SharedFileTree ft, String name, @Nullable String path) {
        assert name != null : "Snapshot name cannot be empty or null.";

        return path == null ? new File(ft.snapshotsRoot(), name) : new File(path, name);
    }

    /**
     * Node file tree for incremental snapshots.
     */
    public class IncrementalSnapshotFileTree extends NodeFileTree {
        /** Increment index. */
        private final int idx;

        /**
         * @param root Root directory.
         * @param folderName Folder name.
         */
        public IncrementalSnapshotFileTree(File root, String folderName, int idx) {
            super(root, folderName);

            this.idx = idx;
        }

        /**
         * @return Increment index.
         */
        public int index() {
            return idx;
        }

        /**
         * @return Path to the meta file.
         */
        public File meta() {
            return new File(root, snapshotMetaFileName(folderName()));
        }

        /** {@inheritDoc} */
        @Override public File walSegment(long idx) {
            return new File(wal(), U.fixedLengthNumberName(idx, ZIP_WAL_SEG_FILE_EXT));
        }
    }

    /**
     * Modifies {@link #drStorages} for this tree to reflect snapshot options.
     * In case {@link IgniteConfiguration#getSnapshotPath()} points to absolute directory or {@link #path} for snapshot provided
     * then all snapshot files must be stored inside one folder.
     * Otherwise, we use configured by {@link DataRegionConfiguration#getStoragePath()} structure to save snapshot.
     * This will distribute workload to all physical device on host.
     *
     * @param ft Node file tree.
     * @param snpDfltPath Snapshot default path.
     */
    private Map<String, File> snapshotDataRegionStorages(NodeFileTree ft, String snpDfltPath) {
        // If path provided then create snapshot inside it, only.
        // Same rule applies if absolute path to the snapshot root dir configured.
        if (path != null || new File(snpDfltPath).isAbsolute())
            return Collections.emptyMap();

        Map<String, File> snpDrStorages = new HashMap<>();

        ft.dataRegionStorages().forEach((drName, drStoragePath) -> {
            // drStorages contains path with the DB and folderName.
            File drStorage = ft.dataRegionStorages().get(drName);

            // In case we want to make snapshot in several folders the pathes will be the following:
            // {dr_storage_path}/db/{folder_name} - node cache storage.
            // {dr_storage_path}/snapshots/{snp_name}/db/{folder_name} - snapshot cache storage.
            snpDrStorages.put(
                drName,
                new File(drStorage.getParentFile().getParentFile(), Path.of(snpDfltPath, name, DB_DIR, folderName()).toString())
            );
        });

        return snpDrStorages;
    }

    /**
     * Creates file tree for temporary files.
     * Required, only for snapshot operation inside running node.
     *
     * @param ctx Kernal context.
     * @return File tree for temporary files.
     * @see NodeFileTree#SNAPSHOT_TMP_DIR
     */
    private NodeFileTree tempFileTree(GridKernalContext ctx) {
        NodeFileTree ft = ctx.pdsFolderResolver().fileTree();

        NodeFileTree tmpFt = new NodeFileTree(ctx.config(), new File(ft.snapshotTempRoot(ft.nodeStorage()), name), folderName());

        Map<String, File> snpTmpDrStorages = new HashMap<>();

        // Iterating via snapshot data region storage,
        // because they may differ from the node one in case snapshot created using absolute path.
        drStorages.forEach((drName, drStoragePath) -> snpTmpDrStorages.put(
            drName,
            new File(ft.dataRegionStorages().get(drName), Path.of(SNAPSHOT_TMP_DIR, name).toString())
        ));

        tmpFt.drStorages.putAll(snpTmpDrStorages);

        return tmpFt;
    }
}
