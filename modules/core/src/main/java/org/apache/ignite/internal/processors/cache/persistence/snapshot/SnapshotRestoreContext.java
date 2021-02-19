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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;

/**
 * Cache group restore from snapshot operation context.
 */
class SnapshotRestoreContext {
    /** Request ID. */
    private final UUID reqId;

    /** Snapshot name. */
    private final String snpName;

    /** Baseline node IDs that must be alive to complete the operation. */
    private final Set<UUID> reqNodes;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** List of processed cache IDs. */
    private final Set<Integer> cacheIds = new HashSet<>();

    /** Cache configurations. */
    private final List<StoredCacheData> ccfgs;

    /** Restored cache groups. */
    private final Map<String, List<File>> grps = new ConcurrentHashMap<>();

    /** The exception that led to the interruption of the process. */
    private final AtomicReference<Throwable> errRef = new AtomicReference<>();

    /**
     * @param reqId Request ID.
     * @param snpName Snapshot name.
     * @param reqNodes Baseline node IDs that must be alive to complete the operation.
     * @param configs Stored cache configurations.
     * @param ctx Kernal context.
     */
    protected SnapshotRestoreContext(UUID reqId, String snpName, Set<UUID> reqNodes, List<StoredCacheData> configs,
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
        this.reqNodes = new HashSet<>(reqNodes);
        this.snpName = snpName;
        this.ctx = ctx;
    }

    /** @return Request ID. */
    protected UUID requestId() {
        return reqId;
    }

    /** @return Baseline node IDs that must be alive to complete the operation. */
    protected Set<UUID> nodes() {
        return Collections.unmodifiableSet(reqNodes);
    }

    /** @return Snapshot name. */
    protected String snapshotName() {
        return snpName;
    }

    /**
     * @return List of cache group names to restore from the snapshot.
     */
    protected Set<String> groups() {
        return grps.keySet();
    }

    /**
     * @param name Cache name.
     * @return {@code True} if the cache with the specified name is currently being restored.
     */
    protected boolean containsCache(String name) {
        return cacheIds.contains(CU.cacheId(name));
    }

    /** @return Cache configurations. */
    protected Collection<StoredCacheData> configs() {
        return ccfgs;
    }

    /**
     * @param err Error.
     * @return {@code True} if operation has been interrupted by this call.
     */
    protected boolean interrupt(Exception err) {
        return errRef.compareAndSet(null, err);
    }

    /**
     * @return Interrupted flag.
     */
    protected boolean interrupted() {
        return error() != null;
    }

    /**
     * @return Error if operation was interrupted, otherwise {@code null}.
     */
    protected @Nullable Throwable error() {
        return errRef.get();
    }

    /**
     * Restore specified cache groups from the local snapshot directory.
     *
     * @param updateMetadata Update binary metadata flag.
     * @throws IgniteCheckedException If failed.
     */
    protected void restore(boolean updateMetadata) throws IgniteCheckedException {
        if (interrupted())
            return;

        IgniteSnapshotManager snapshotMgr = ctx.cache().context().snapshotMgr();

        if (updateMetadata) {
            File binDir = binaryWorkDir(snapshotMgr.snapshotLocalDir(snpName).getAbsolutePath(),
                ctx.pdsFolderResolver().resolveFolders().folderName());

            if (!binDir.exists()) {
                throw new IgniteCheckedException("Unable to update cluster metadata from snapshot, " +
                    "directory doesn't exists [snapshot=" + snpName + ", dir=" + binDir + ']');
            }

            ctx.cacheObjects().updateMetadata(binDir, this::interrupted);
        }

        for (String grpName : groups())
            snapshotMgr.restoreCacheGroupFiles(snpName, grpName, this::interrupted, grps.get(grpName));
    }

    /**
     * Rollback changes made by process in specified cache group.
     */
    protected void rollback() {
        if (groups().isEmpty())
            return;

        List<String> grpNames = new ArrayList<>(groups());

        for (String grpName : grpNames) {
            List<File> files = grps.remove(grpName);

            if (files != null)
                ctx.cache().context().snapshotMgr().rollbackRestoreOperation(files);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRestoreContext.class, this);
    }
}
