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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;

/**
 * Cache group restore from snapshot operation context.
 */
class SnapshotRestoreContext {
    /** Request ID. */
    private final UUID reqId;

    /** Snapshot name. */
    private final String snpName;

    /** List of baseline node IDs that must be alive to complete the operation. */
    private final Set<UUID> reqNodes;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Restore operation lock. */
    private final ReentrantLock rollbackLock = new ReentrantLock();

    /** Cache configurations. */
    private final Map<Integer, StoredCacheData> cacheCfgs = new ConcurrentHashMap<>();

    /** Restored cache groups. */
    private final Map<String, GroupRestoreContext> grps = new ConcurrentHashMap<>();

    /**
     * @param reqId Request ID.
     * @param snpName Snapshot name.
     * @param reqNodes List of baseline node IDs that must be alive to complete the operation.
     * @param configs Stored cache configurations.
     * @param ctx Kernal context.
     */
    public SnapshotRestoreContext(UUID reqId, String snpName, Set<UUID> reqNodes, List<StoredCacheData> configs,
        GridKernalContext ctx) {
        for (StoredCacheData cacheData : configs) {
            String cacheName = cacheData.config().getName();

            cacheCfgs.put(CU.cacheId(cacheName), cacheData);

            boolean shared = cacheData.config().getGroupName() != null;

            grps.computeIfAbsent(
                shared ? cacheData.config().getGroupName() : cacheName, v -> new GroupRestoreContext(shared));
        }

        this.reqId = reqId;
        this.reqNodes = reqNodes;
        this.snpName = snpName;
        this.ctx = ctx;
    }

    /** @return Request ID. */
    public UUID requestId() {
        return reqId;
    }

    /** @return List of baseline node IDs that must be alive to complete the operation. */
    public Set<UUID> nodes() {
        return Collections.unmodifiableSet(reqNodes);
    }

    /** @return Snapshot name. */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return List of cache group names to restore from the snapshot.
     */
    public Set<String> groups() {
        return grps.keySet();
    }

    /**
     * @return Names of the directories of the restored caches.
     */
    public Collection<String> groupDirs() {
        return F.viewReadOnly(grps.entrySet(),
            e -> (e.getValue().shared ? CACHE_GRP_DIR_PREFIX : CACHE_DIR_PREFIX) + e.getKey());
    }

    /**
     * @param name Cache name.
     * @return {@code True} if the cache with the specified name is currently being restored.
     */
    public boolean containsCache(String name) {
        return grps.containsKey(name) || cacheCfgs.containsKey(CU.cacheId(name));
    }

    /** @return Cache configurations. */
    public Collection<StoredCacheData> configs() {
        return cacheCfgs.values();
    }

    /**
     * Restore specified cache groups from the local snapshot directory.
     *
     * @param updateMetadata Update binary metadata flag.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @throws IgniteCheckedException If failed.
     */
    public void restore(boolean updateMetadata, BooleanSupplier stopChecker) throws IgniteCheckedException {
        if (stopChecker.getAsBoolean())
            return;

        if (updateMetadata) {
            File binDir = binaryWorkDir(ctx.cache().context().snapshotMgr().snapshotLocalDir(snpName).getAbsolutePath(),
                ctx.pdsFolderResolver().resolveFolders().folderName());

            if (!binDir.exists()) {
                throw new IgniteCheckedException("Unable to update cluster metadata from snapshot, " +
                    "directory doesn't exists [snapshot=" + snpName + ", dir=" + binDir + ']');
            }

            ctx.cacheObjects().updateMetadata(binDir, stopChecker);
        }

        for (String grpName : groups()) {
            rollbackLock.lock();

            try {
                GroupRestoreContext grp = grps.get(grpName);

                ctx.cache().context().snapshotMgr().restoreCacheGroupFiles(snpName, grpName, stopChecker, grp.files);
            }
            finally {
                rollbackLock.unlock();
            }
        }
    }

    /**
     * Rollback changes made by process in specified cache group.
     */
    public void rollback() {
        rollbackLock.lock();

        try {
            List<String> grpNames = new ArrayList<>(groups());

            for (String grpName : grpNames) {
                GroupRestoreContext grp = grps.remove(grpName);

                if (grp != null)
                    ctx.cache().context().snapshotMgr().rollbackRestoreOperation(grp.files);
            }
        }
        finally {
            rollbackLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRestoreContext.class, this);
    }

    /** */
    private static class GroupRestoreContext {
        /** Files created in the cache group folder during a restore operation. */
        final List<File> files = new ArrayList<>();

        final boolean shared;

        private GroupRestoreContext(boolean shared) {
            this.shared = shared;
        }
    }
}
