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
import java.nio.file.Paths;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_CDC_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.DB_DEFAULT_FOLDER;

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
 * ├── cp
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
 * │  │  ├── cache-default
 * │  │  │  ├── cache_data.dat
 * │  │  │  ├── index.bin
 * │  │  │  ├── part-0.bin
 * │  │  │  ├── part-1.bin
 * ...
 * │  │  │  └── part-9.bin
 * │  │  ├── cache-ignite-sys-cache
 * │  │  │  ├── cache_data.dat
 * │  │  │  └── index.bin
 * │  │  ├── cache-tx-cache
 * │  │  │  ├── cache_data.dat
 * │  │  │  ├── index.bin
 * │  │  │  ├── part-0.bin
 * │  │  │  ├── part-1.bin
 * ...
 * │  │  │  └── part-9.bin
 * │  │  ├── cp
 * │  │  │  ├── 1737804007693-96128bb0-5361-495a-b593-53dc4339a56d-END.bin
 * │  │  │  └── 1737804007693-96128bb0-5361-495a-b593-53dc4339a56d-START.bin
 * │  │  ├── lock
 * │  │  ├── maintenance_tasks.mntc
 * │  │  ├── metastorage
 * │  │  │  ├── part-0.bin
 * │  │  │  └── part-1.bin
 * │  │  └── snp                                                                ← snpTmp (node 0)
 * │  ├── node01-e57e62a9-2ccf-4e1b-a11e-d35d32c0fe5d                           ← nodeStorage (node 1).
 * │  │  ├── cache-default
 * ..
 * │  │  ├── cache-ignite-sys-cache
 * ...
 * │  │  ├── cache-tx-cache
 * ...
 * │  │  ├── cp
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
 *
 * │      │  └── node01-e57e62a9-2ccf-4e1b-a11e-d35d32c0fe5d                    ← walCdc (node 1)
 * │      └── node00-e57e62a9-2ccf-4e1b-a11e-c24c21b9ed4c                       ← wal (node 0)
 * │          ├── 0000000000000000.wal
 * │          ├── 0000000000000001.wal
 * ...
 * │          └── 0000000000000009.wal
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
 * └── snapshots                                                                ← snpsRoot (shared between all nodes).
 * </pre>
 */
public class NodeFileTree extends SharedFileTree {
    /** Default snapshot directory for loading remote snapshots. */
    public static final String DFLT_SNAPSHOT_TMP_DIR = "snp";

    /** Folder name for consistent id. */
    private final String folderName;

    /** Path to the directory containing binary metadata. */
    private final File binaryMeta;

    /** Path to the storage directory. */
    private final @Nullable File nodeStorage;

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
        snpTmpRoot = new File(nodeStorage, DFLT_SNAPSHOT_TMP_DIR);
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
            wal = resolveDirectory(dsCfg.getWalPath());
            walArchive = resolveDirectory(dsCfg.getWalArchivePath());
            walCdc = resolveDirectory(dsCfg.getCdcWalPath());
            snpTmpRoot = new File(nodeStorage, DFLT_SNAPSHOT_TMP_DIR);
        }
        else {
            nodeStorage = null;
            wal = null;
            walArchive = null;
            walCdc = null;
            snpTmpRoot = null;
        }
    }

    /** @return Node storage directory. */
    public File nodeStorage() {
        return nodeStorage;
    }

    /** @return Path to binary metadata directory. */
    public File binaryMeta() {
        return binaryMeta;
    }

    /** @return Path to the directory containing active WAL segments. */
    public @Nullable File wal() {
        return wal;
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
    public File snapshotTempRoot() {
        return snpTmpRoot;
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

    /** @return {@code True} if WAL archive enabled. */
    public boolean walArchiveEnabled() {
        return walArchive != null && wal != null && !walArchive.equals(wal);
    }

    /**
     * Creates a directory specified by the given arguments.
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
