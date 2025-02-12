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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.MAX_PARTITION_ID;

/**
 * {@link NodeFileTree} extension with the methods required to work with snapshot file tree.
 * During creation, full snapshot, creates the same file tree as regular node.
 * But, using snapshot directory as root.
 */
public class SnapshotFileTree extends NodeFileTree {
    /** File with delta pages suffix. */
    public static final String DELTA_SUFFIX = ".delta";

    /** File with delta pages index suffix. */
    public static final String DELTA_IDX_SUFFIX = ".idx";

    /** File name template consists of delta pages. */
    public static final String PART_DELTA_TEMPLATE = PART_FILE_TEMPLATE + DELTA_SUFFIX;

    /** File name template for index delta pages. */
    public static final String INDEX_DELTA_NAME = INDEX_FILE_NAME + DELTA_SUFFIX;

    /** Lock file for dump directory. */
    public static final String DUMP_LOCK = "dump.lock";

    /** Incremental snapshots directory name. */
    public static final String INC_SNP_DIR = "increments";

    /** Snapshot name. */
    private final String name;

    /** Snapshot path. */
    @Nullable private final String path;

    /** Node file tree relative to {@link #tempFileTree()}. */
    private final NodeFileTree tmpFt;

    /**
     * @param loc Local node.
     * @param name Snapshot name.
     * @param path Optional snapshot path.
     */
    public SnapshotFileTree(IgniteEx loc, String name, @Nullable String path) {
        this(loc.context().pdsFolderResolver().fileTree(), name, path);
    }

    /**
     * @param ft Node file tree.
     * @param name Snapshot name.
     * @param path Optional snapshot path.
     */
    public SnapshotFileTree(NodeFileTree ft, String name, @Nullable String path) {
        this(ft, ft.folderName(), name, path);
    }

    /**
     * @param ft Node file tree.
     * @param folderName Folder name (for the cases when snapshot can be copied from other node).
     * @param name Snapshot name.
     * @param path Optional snapshot path.
     */
    public SnapshotFileTree(NodeFileTree ft, String folderName, String name, @Nullable String path) {
        super(root(ft, name, path), folderName);

        A.notNullOrEmpty(name, "Snapshot name cannot be null or empty.");
        A.ensure(U.alphanumericUnderscore(name), "Snapshot name must satisfy the following name pattern: a-zA-Z0-9_");

        this.name = name;
        this.path = path;
        this.tmpFt = new NodeFileTree(new File(ft.snapshotTempRoot(), name), folderName);
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
     * @param incIdx Increment index.
     * @return Root directory for incremental snapshot.
     */
    public NodeFileTree incrementalSnapshotFileTree(int incIdx) {
        return new NodeFileTree(new File(incrementsRoot(), U.fixedLengthNumberName(incIdx, null)), U.maskForFileName(folderName()));
    }

    /**
     * @param cacheDirName Cache dir name.
     * @param partId Cache partition identifier.
     * @return A file representation.
     */
    public File partDeltaFile(String cacheDirName, int partId) {
        return new File(tmpFt.cacheStorage(cacheDirName), partDeltaFileName(partId));
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
     * @param partId Partition id.
     * @return File name of delta partition pages.
     */
    public static String partDeltaFileName(int partId) {
        assert partId <= MAX_PARTITION_ID || partId == INDEX_PARTITION;

        return partId == INDEX_PARTITION ? INDEX_DELTA_NAME : String.format(PART_DELTA_TEMPLATE, partId);
    }

    /**
     * @param ft Node file tree.
     * @param name Snapshot name.
     * @param path Optional snapshot path.
     * @return Path to the snapshot root directory.
     */
    private static File root(NodeFileTree ft, String name, @Nullable String path) {
        assert name != null : "Snapshot name cannot be empty or null.";

        return path == null ? new File(ft.snapshotsRoot(), name) : new File(path, name);
    }
}
