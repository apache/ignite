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

package org.apache.ignite.internal.processors.cache.persistence.backup;

import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;

/** */
public interface IgniteBackupPageStoreManager<T> extends GridCacheSharedManager, IgniteChangeGlobalStateSupport {
    /**
     * Take backup of specified cache group partition files and syncronously wait to its completion.
     *
     * @param idx Unique process identifier.
     * @param grpId Tracked cache group id.
     * @param parts Tracking cache group partitions during snapshot.
     * @param hndlr Handler for processing partitions and corresponding partition deltas.
     * @throws IgniteCheckedException If fails.
     */
    public void backup(
        int idx,
        int grpId,
        Set<Integer> parts,
        BackupProcessTask<T> hndlr
    ) throws IgniteCheckedException;

    /**
     * @param pairId Cache group, partition identifiers pair.
     * @param store Store to handle operatwion at.
     * @param pageId Tracked page id.
     */
    public void handleWritePageStore(GroupPartitionId pairId, PageStore store, long pageId);

    /**
     * @param grpId Cache group to init temorary store for.
     * @param partIds Cache partitions to init temporary stores for.
     * @throws IgniteCheckedException If fails.
     */
    public void initTemporaryStores(int grpId, Set<Integer> partIds) throws IgniteCheckedException;
}
