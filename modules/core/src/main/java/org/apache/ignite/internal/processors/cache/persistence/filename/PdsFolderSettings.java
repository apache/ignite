/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.File;
import java.io.Serializable;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Class holds information required for folder generation for ignite persistent store
 */
public class PdsFolderSettings {
    /**
     * DB storage absolute root path resolved as 'db' folder in Ignite work dir (by default) or using persistent store
     * configuration. <br>
     * Note WAL storage may be configured outside this path.<br>
     * This value may be null if persistence is not enabled.
     */
    @Nullable private final File persistentStoreRootPath;

    /** Sub folder name containing consistent ID and optionally node index. */
    private final String folderName;

    /** Consistent id to be set to local node. */
    private final Serializable consistentId;

    /**
     * File lock holder with prelocked db directory. For non compatible mode this holder contains prelocked work
     * directory. This value is to be used at activate instead of locking. <br> May be null in case preconfigured
     * consistent ID is used or in case lock holder was already taken by other processor.
     */
    @Nullable private final GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder;

    /**
     * Indicates if compatible mode is enabled, in that case all sub folders are generated from consistent ID without
     * 'node' and node index prefix. In compatible mode there is no overriding for consistent ID is done.
     */
    private final boolean compatible;

    /**
     * Creates settings in for new PST(DB) folder mode.
     *
     * @param persistentStoreRootPath Persistent store root path or null if non PDS mode.
     * @param folderName Sub folder name containing consistent ID and optionally node index.
     * @param consistentId Consistent id.
     * @param fileLockHolder File lock holder with prelocked db directory.
     * @param compatible Compatible mode flag.
     */
    public PdsFolderSettings(@Nullable final File persistentStoreRootPath,
        final String folderName,
        final Serializable consistentId,
        @Nullable final GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder,
        final boolean compatible) {

        this.consistentId = consistentId;
        this.folderName = folderName;
        this.fileLockHolder = fileLockHolder;
        this.compatible = compatible;
        this.persistentStoreRootPath = persistentStoreRootPath;
    }

    /**
     * Creates settings for compatible mode. Folder name is consistent ID (masked), no node prefix is added.
     *
     * @param persistentStoreRootPath root DB path.
     * @param consistentId node consistent ID.
     */
    public PdsFolderSettings(
        @Nullable final File persistentStoreRootPath,
        @NotNull final Serializable consistentId) {

        this.consistentId = consistentId;
        this.compatible = true;
        this.folderName = U.maskForFileName(consistentId.toString());
        this.persistentStoreRootPath = persistentStoreRootPath;
        this.fileLockHolder = null;
    }

    /**
     * @return sub folders name based on consistent ID. In compatible mode this is escaped consistent ID, in new mode
     * this is UUID based folder name.
     */
    public String folderName() {
        return folderName;
    }

    /**
     * @return Consistent id to be set to local node.
     */
    public Serializable consistentId() {
        return consistentId;
    }

    /**
     * @return flag indicating if compatible mode is enabled for folder generation. In that case all sub folders names are
     * generated from consistent ID without 'node' and node index prefix. In compatible mode there is no overriding for
     * consistent ID is done for cluster node. Locking files is independent to compatibility mode.
     */
    public boolean isCompatible() {
        return compatible;
    }

    /**
     * Returns already locked file lock holder to lock file in {@link #persistentStoreRootPath}. Unlock and close this
     * lock is not required.
     *
     * @return File lock holder with prelocked db directory.
     */
    @Nullable public GridCacheDatabaseSharedManager.FileLockHolder getLockedFileLockHolder() {
        return fileLockHolder;
    }

    /**
     * @return DB storage absolute root path resolved as 'db' folder in Ignite work dir (by default) or using persistent
     * store configuration. Note WAL storage may be configured outside this path. May return null for non pds mode.
     */
    @Nullable public File persistentStoreRootPath() {
        return persistentStoreRootPath;
    }
}
