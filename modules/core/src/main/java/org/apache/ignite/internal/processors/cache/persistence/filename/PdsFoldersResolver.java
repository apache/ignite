/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.filename;

import org.apache.ignite.IgniteCheckedException;

/**
 * Resolves folders for PDS mode, may have side effect as setting random UUID as local node consistent ID.
 */
public interface PdsFoldersResolver {
    /**
     * Prepares and caches PDS folder settings. Subsequent call to this method will provide same settings.
     *
     * @return PDS folder settings, consistentID and prelocked DB file lock.
     * @throws IgniteCheckedException if failed.
     */
    public PdsFolderSettings resolveFolders() throws IgniteCheckedException;
}
