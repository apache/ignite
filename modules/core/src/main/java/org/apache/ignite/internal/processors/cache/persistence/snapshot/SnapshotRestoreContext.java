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
import java.util.HashSet;
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
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;

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

    /** List of processed cache IDs. */
    private final Set<Integer> cacheIds = new HashSet<>();

    /** Cache configurations. */
    private final List<StoredCacheData> ccfgs;

    /** Restored cache groups. */
    private final Map<String, List<File>> grps = new ConcurrentHashMap<>();

    /**
     * @param reqId Request ID.
     * @param snpName Snapshot name.
     * @param reqNodes List of baseline node IDs that must be alive to complete the operation.
     * @param configs Stored cache configurations.
     * @param ctx Kernal context.
     */
    public SnapshotRestoreContext(UUID reqId, String snpName, Set<UUID> reqNodes, List<StoredCacheData> configs,
        GridKernalContext ctx) {
        ccfgs = new ArrayList<>(configs);

        for (StoredCacheData cacheData : configs) {
            String cacheName = cacheData.config().getName();

            cacheIds.add(CU.cacheId(cacheName));

            boolean shared = cacheData.config().getGroupName() != null;

            grps.computeIfAbsent(shared ? cacheData.config().getGroupName() : cacheName, v -> new ArrayList<>());

            if (shared)
                cacheIds.add(CU.cacheId(cacheData.config().getGroupName()));
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
     * @param name Cache name.
     * @return {@code True} if the cache with the specified name is currently being restored.
     */
    public boolean containsCache(String name) {
        return cacheIds.contains(CU.cacheId(name));
    }

    /** @return Cache configurations. */
    public Collection<StoredCacheData> configs() {
        return ccfgs;
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
                ctx.cache().context().snapshotMgr().restoreCacheGroupFiles(snpName, grpName, stopChecker, grps.get(grpName));
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
                List<File> files = grps.remove(grpName);

                if (files != null)
                    ctx.cache().context().snapshotMgr().rollbackRestoreOperation(files);
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
}
