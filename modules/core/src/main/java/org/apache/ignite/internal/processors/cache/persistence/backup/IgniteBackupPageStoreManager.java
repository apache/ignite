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

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;

/** */
public interface IgniteBackupPageStoreManager extends GridCacheSharedManager, IgniteChangeGlobalStateSupport {
    /**
     * @param backupName Unique backup identifier.
     * @param parts Collection of pairs group and appropratate cache partition to be backuped.
     * @param closure Partition backup handling closure.
     * @throws IgniteCheckedException If fails.
     */
    public void backup(
        String backupName,
        Map<Integer, Set<Integer>> parts,
        PageStoreInClosure closure
    ) throws IgniteCheckedException;

    /**
     * @return Checkpoint future for scheduled backups.
     */
    public IgniteInternalFuture<Void> forceStart();

    /**
     * @param backupName Unique backup name.
     * @param parts Collection of pairs group and appropratate cache partition to be backuped.
     * @return Future which will be completed when cache is ready to be processed.
     */
    public IgniteInternalFuture<Void> setupCacheBackup(String backupName, Map<Integer, Set<Integer>> parts);

    /**
     * @param backupName Unique backup name.
     * @param dir Local directory to save cache partition deltas to.
     * @param executor Executor to use for async backup execution.
     * @return Future which will be completed when backup is done.
     */
    public IgniteInternalFuture<?> runLocalBackup(String backupName, File dir, Executor executor);

    /**
     * @param backupName Unique backup name.
     * @param cctx Cache context to use to.
     */
    public void stopCacheBackup(String backupName, GridCacheContext cctx);

    /**
     * @param pairId Cache group, partition identifiers pair.
     * @param store Store to handle operatwion at.
     * @param pageId Tracked page id.
     */
    public void beforeStoreWrite(GroupPartitionId pairId, PageStore store, long pageId);
}
