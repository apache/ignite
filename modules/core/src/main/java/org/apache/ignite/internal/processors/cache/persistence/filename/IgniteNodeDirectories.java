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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Provides access to Ignite node directories.
 * Note, that base path can be different for each usage:
 * <ul>
 *     <li>Ignite node.</li>
 *     <li>Snapshot files.</li>
 *     <li>Cache dump files.</li>
 *     <li>CDC.</li>
 * </ul>
 *
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
 * │  ├── node00-e57e62a9-2ccf-4e1b-a11e-c24c21b9ed4c                           ← folderName (node 0).
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
 * │  ├── node01-e57e62a9-2ccf-4e1b-a11e-d35d32c0fe5d                           ← folderName (node 1).
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
 * └── snapshots                                                                ← snapshotRoot (shared between all local nodes).
 * </pre>
 */
public class IgniteNodeDirectories extends IgniteSharedDirectories {
    /** Folder name for consistent id. */
    private final String folderName;

    /** Path to the directory containing binary metadata. */
    private final File binaryMeta;

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
    public IgniteNodeDirectories(String root, String folderName) {
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
    public IgniteNodeDirectories(File root, String folderName) {
        super(root);

        A.notNullOrEmpty(folderName, "Node directory");

        this.folderName = folderName;

        binaryMeta = new File(binaryMetaRoot.getAbsolutePath(), folderName);
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
    public IgniteNodeDirectories(IgniteConfiguration cfg, String folderName) {
        super(cfg);

        A.notNullOrEmpty(folderName, "Node directory");

        this.folderName = folderName;

        binaryMeta = new File(binaryMetaRoot, folderName);
    }

    /** @return Folder name. */
    public String folderName() {
        return folderName;
    }

    /** @return Path to binary metadata directory. */
    public File binaryMeta() {
        return binaryMeta;
    }

    /**
     * Creates {@link #binaryMeta()} directory.
     * @return Created directory.
     * @see #binaryMeta()
     */
    public File mkdirBinaryMeta() {
        return mkdir(binaryMeta, "binary metadata");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteNodeDirectories.class, this);
    }
}
