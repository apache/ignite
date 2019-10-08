package org.apache.ignite.internal.processors.cache.preload;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;

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
        //BackupProcessSupplier hndlr,
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
