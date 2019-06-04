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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;

/** */
public interface IgniteBackupPageStoreManager extends GridCacheSharedManager, IgniteChangeGlobalStateSupport {
    /**
     * @param name The unique backup name.
     * @param parts Collection of pairs group and appropratate cache partition to be backuped.
     * @param backupClsr Partition backup handling closure.
     * @return Future will be finished when backup ends.
     */
    public IgniteInternalFuture<Boolean> localBackup(
        String name,
        Map<Integer, Set<Integer>> parts,
        BackupInClosure backupClsr
    );

    /**
     * @param pairId Cache group, partition identifiers pair.
     * @param store Store to handle operatwion at.
     * @param pageId Tracked page id.
     */
    public void beforePageWritten(GroupPartitionId pairId, PageStore store, long pageId);
}
