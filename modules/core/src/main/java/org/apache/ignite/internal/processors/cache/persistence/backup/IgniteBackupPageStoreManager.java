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

import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;

/** */
public interface IgniteBackupPageStoreManager extends GridCacheSharedManager, IgniteChangeGlobalStateSupport {
    /**
     * Take backup of specified cache group partition files and syncronously wait to its completion.
     *
     * @param idx Unique process identifier.
     * @param grpsBackup Backing up cache groups and corresponding partitions.
     * @param hndlr Handler for processing partitions and corresponding partition deltas.
     * @param fut A future of process flow control.
     * @throws IgniteCheckedException If fails.
     */
    public void backup(
        long idx,
        Map<Integer, Set<Integer>> grpsBackup,
        BackupProcessSupplier hndlr,
        IgniteInternalFuture<Boolean> fut
    ) throws IgniteCheckedException;

    /**
     * @param pairId Cache group, partition identifiers pair.
     * @param store Store to handle operatwion at.
     * @param pageId Tracked page id.
     */
    public void handleWritePageStore(GroupPartitionId pairId, PageStore store, long pageId);

    /**
     * @param grpPartIdSet Collection of pairs cache group and partition ids.
     * @throws IgniteCheckedException If fails.
     */
    public void initTemporaryStores(Set<GroupPartitionId> grpPartIdSet) throws IgniteCheckedException;
}
