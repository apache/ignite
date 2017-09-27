/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.filename;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class holds information required for folder generation for ignite persistent store
 */
public class PdsFolderSettings {
    private final Serializable consistentId;

    /** folder name containing consistent ID and optionally node index */
    private final String folderName;
    /**
     * File lock holder with prelocked db directory. For non compatible mode this holder contains prelocked work
     * directory. This value is to be used at activate instead of locking.
     */
    private GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder;

    /**
     * Indicates if compatible mode is enabled, in that case all subfolders are generated from consistent ID without
     * 'node' and node index prefix.
     */
    private final boolean compatible;

    public PdsFolderSettings(Serializable consistentId, boolean compatible) {
        this.consistentId = consistentId;
        this.compatible = compatible;
        this.folderName = U.maskForFileName(consistentId.toString());
    }

    public PdsFolderSettings(UUID consistentId, String folderName, int nodeIdx,
        GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder,
        boolean compatible) {
        this.consistentId = consistentId;
        this.folderName = folderName;
        this.fileLockHolder = fileLockHolder;
        this.compatible = compatible;
    }

    public String folderName() {
        return folderName;
    }

    public Serializable consistentId() {
        return consistentId;
    }

    public boolean isCompatible() {
        return compatible;
    }

    /**
     * @return File lock holder with prelocked db directory
     */
    public GridCacheDatabaseSharedManager.FileLockHolder fileLockHolder() {
        return fileLockHolder;
    }
}
