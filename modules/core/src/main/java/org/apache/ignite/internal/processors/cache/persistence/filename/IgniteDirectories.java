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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_CDC_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.DB_DEFAULT_FOLDER;

/**
 * Class contains pathes to Ignite folder.
 * Note, that base path can be different for each usage:
 * <ul>
 *     <li>Ignite node.</li>
 *     <li>Snapshot files.</li>
 *     <li>Cache dump files</li>
 *     <li>CDC</li>
 * </ul>
 * Ignite node directories structure with the point to currenlty supported dirs.
 * Description:<br>
 * <ul>
 *     <li>{@code .} folder is {@code root} constructor parameter.</li>
 *     <li>{@code node00-e57e62a9-2ccf-4e1b-a11e-c24c21b9ed4c} is the {@link PdsFolderSettings#folderName()} and {@code folderName}
 *     constructor parameter.</li>
 * </ul>
 *
 * <pre>
 * ❯ tree
 * .                                                                            ← root (work directory, shared between all nodes).
 * ├── cp
 * │  └── sharedfs
 * │      └── BinaryMarshaller
 * ├── db                                                                       ← db (shared between all nodes).
 * │  ├── binary_meta                                                           ← binaryMetaRoot
 * │  │  └── node00-e57e62a9-2ccf-4e1b-a11e-c24c21b9ed4c                        ← binaryMeta for node 0
 * │  │      └── 1645778359.bin
 * │  │  └── node01-e57e62a9-2ccf-4e1b-a11e-d35d32c0fe5d                        ← binaryMeta for node 1
 * │  │      └── 1645778359.bin
 * │  ├── lock
 * │  ├── marshaller                                                            ← marshaller (shared between all nodes)
 * │  │  └── 1645778359.classname0
 * │  ├── node00-e57e62a9-2ccf-4e1b-a11e-c24c21b9ed4c
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
 * │  │  └── snp
 * │  ├── node01-e57e62a9-2ccf-4e1b-a11e-d35d32c0fe5d
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
 * │  │  └── snp
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
 * └── snapshots
 * </pre>
 */
public class IgniteDirectories {
    /** Default path (relative to working directory) of binary metadata folder */
    public static final String DFLT_BINARY_METADATA_PATH = "db/binary_meta";

    /** Default path (relative to working directory) of marshaller mappings folder */
    public static final String DFLT_MARSHALLER_PATH = "db/marshaller";

    /** Root(work) directory. */
    private final File root;

    /** Path to the directory containing binary metadata. */
    private final File binaryMetaRoot;

    /** Path to the directory containing binary metadata. */
    private final @Nullable File binaryMeta;

    /** Path to the directory containing marshaller files. */
    private final File marshaller;

    /** Path to the directory containing active WAL segments. */
    private final File wal;

    /** Path to the directory containing archive WAL segments. */
    private final File walArchive;

    /** Path to the directory containing archive WAL segments for CDC. */
    private final File walCdc;

    /**
     * @param root Root directory.
     */
    public IgniteDirectories(File root) {
        A.notNull(root, "Root directory");

        this.root = root;
        marshaller = new File(root, DFLT_MARSHALLER_PATH);
        binaryMetaRoot = new File(root, DFLT_BINARY_METADATA_PATH);
        binaryMeta = null;
        wal = null;
        walArchive = null;
        walCdc = null;
    }

    /**
     * @param root Root directory.
     */
    public IgniteDirectories(String root) {
        this(new File(root));
    }

    /**
     * Note, to calculate all field you need to specify {@code folderName}.
     *
     * @param cfg Ignite configuration.
     * @see IgniteDirectories#IgniteDirectories(IgniteConfiguration, String)
     */
    public IgniteDirectories(IgniteConfiguration cfg) throws IgniteCheckedException {
        this(U.workDirectory(cfg.getWorkDirectory(), cfg.getIgniteHome()));
    }

    /**
     * Root directory can be Ignite work directory, see {@link U#workDirectory(String, String)} and other methods.
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
    public IgniteDirectories(String root, String folderName) {
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
    public IgniteDirectories(File root, String folderName) {
        A.notNull(root, "Root directory");
        A.notNullOrEmpty(folderName, "Node directory");

        this.root = root;
        marshaller = new File(root, DFLT_MARSHALLER_PATH);
        binaryMetaRoot = new File(root, DFLT_BINARY_METADATA_PATH);
        binaryMeta = folderName == null ? null : new File(binaryMetaRoot.getAbsolutePath(), folderName);
        wal = folderName == null ? null : new File(new File(root, DFLT_WAL_PATH), folderName);
        walArchive = folderName == null ? null : new File(new File(root, DFLT_WAL_ARCHIVE_PATH), folderName);
        walCdc = folderName == null ? null : new File(new File(root, DFLT_WAL_CDC_PATH), folderName);
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
    public IgniteDirectories(IgniteConfiguration cfg, String folderName) {
        A.notNull(cfg, "config");
        A.notNullOrEmpty(folderName, "Node directory");

        try {
            root = new File(U.workDirectory(cfg.getWorkDirectory(), cfg.getIgniteHome()));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        marshaller = new File(root, DFLT_MARSHALLER_PATH);
        binaryMetaRoot = new File(root, DFLT_BINARY_METADATA_PATH);
        binaryMeta = new File(binaryMetaRoot, folderName);

        DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

        A.notNull(dsCfg, "Data storage configuration");

        wal = resolveDirectory(folderName, dsCfg.getWalPath());
        walArchive = resolveDirectory(folderName, dsCfg.getWalArchivePath());
        walCdc = resolveDirectory(folderName, dsCfg.getCdcWalPath());
    }

    /**
     * @return Path to the {@code root} directory.
     */
    public File root() {
        return root;
    }

    /**
     * @return Path to the {@code db} directory.
     */
    public File db() {
        return new File(root, DB_DEFAULT_FOLDER);
    }

    /**
     * @return Path to common binary metadata directory. Note, directory can contains data from several nodes.
     * Each node will create own directory inside this root.
     */
    public File binaryMetaRoot() {
        return binaryMetaRoot;
    }

    /** @return Path to binary metadata directory. */
    public File binaryMeta() {
        return binaryMeta;
    }

    /** @return Path to marshaller directory. TODO add example. */
    public File marshaller() {
        return marshaller;
    }

    /** @return Path to the directory containing active WAL segments. */
    public File wal() {
        return wal;
    }

    /** @return Path to the directory containing archive WAL segments. */
    public File walArchive() {
        return walArchive;
    }

    /** @return Path to the directory containing archive WAL segments for CDC. */
    public File walCdc() {
        return walCdc;
    }

    /**
     * Creates {@link #binaryMeta()} directory.
     * @return Created directory.
     * @see #binaryMeta()
     */
    public File mkdirBinaryMetaRoot() throws IgniteCheckedException {
        return mkdir(binaryMetaRoot, "root binary metadata");
    }

    /**
     * Creates {@link #binaryMeta()} directory.
     * @return Created directory.
     * @see #binaryMeta()
     */
    public File mkdirBinaryMeta() throws IgniteCheckedException {
        return mkdir(binaryMeta, "binary metadata");
    }

    /**
     * Creates {@link #marshaller()} directory.
     * @return Created directory.
     * @see #marshaller()
     */
    public File mkdirMarshaller() throws IgniteCheckedException {
        return mkdir(marshaller, "marshaller mappings");
    }

    /**
     * @param f File to check.
     * @return {@code True} if argument can be binary meta root directory.
     */
    public static boolean isBinaryMetaRoot(File f) {
        return f.getAbsolutePath().endsWith(DFLT_BINARY_METADATA_PATH);
    }

    /**
     * @param f File to check.
     * @return {@code True} if f ends with binary meta root directory.
     */
    public static boolean isMarshaller(File f) {
        return f.getAbsolutePath().endsWith(DFLT_MARSHALLER_PATH);
    }

    /**
     * @param file File to check.
     * @return {@code True} if {@code f} contains binary meta root directory.
     */
    public static boolean containsBinaryMetaPath(File file) {
        return file.getPath().contains(DFLT_BINARY_METADATA_PATH);
    }

    /**
     * @param f File to check.
     * @return {@code True} if {@code f} contains marshaller directory.
     */
    public static boolean containsMarshaller(File f) {
        return f.getAbsolutePath().contains(DFLT_MARSHALLER_PATH);
    }

    /**
     * Creates a directory specified by the given arguments.
     *
     * @param folderName Local node consistent ID.
     * @param cfg        Configured directory path, may be {@code null}.
     * @return Initialized directory.
     * @throws IgniteCheckedException If failed to initialize directory.
     */
    private File resolveDirectory(String folderName, String cfg) {
        File sharedBetweenNodesDir = new File(cfg);

        if (!sharedBetweenNodesDir.isAbsolute())
            sharedBetweenNodesDir = new File(root, cfg);

        return new File(sharedBetweenNodesDir, folderName);
    }

    /**
     * @param dir Directory to create
     * @throws IgniteCheckedException
     */
    private File mkdir(File dir, String name) throws IgniteCheckedException {
        if (!U.mkdirs(dir))
            throw new IgniteException("Could not create directory for " + name + ": " + dir);

        if (!dir.canRead())
            throw new IgniteCheckedException("Cannot read from directory: " + dir);

        if (!dir.canWrite())
            throw new IgniteCheckedException("Cannot write to directory: " + dir);

        return dir;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDirectories.class, this);
    }
}
