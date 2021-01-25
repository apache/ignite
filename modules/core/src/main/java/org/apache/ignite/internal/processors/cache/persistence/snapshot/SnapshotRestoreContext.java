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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Cache restore from snapshot operation context.
 */
class SnapshotRestoreContext {
    private final Map<String, List<File>> newGrpFiles = new HashMap<>();

    private final Map<String, PendingStartCacheGroup> pendingStartCaches = new ConcurrentHashMap<>();

    private final Set<Integer> cacheIds = new GridConcurrentHashSet<>();

    /** Request ID. */
    private final UUID reqId;

    /** Snapshot name. */
    private final String snpName;

    /** List of baseline node IDs that must be alive to complete the operation. */
    private final Set<UUID> reqNodes;

    /** Snapshot manager. */
    private final IgniteSnapshotManager snapshotMgr;

    private volatile Collection<StoredCacheData> cacheCfgsToStart;

    /** Restore operation lock. */
    private final ReentrantLock rollbackLock = new ReentrantLock();

    /**
     * @param reqId Request ID.
     * @param snpName Snapshot name.
     * @param reqNodes List of baseline node IDs that must be alive to complete the operation.
     * @param grps List of cache group names to restore from the snapshot.
     * @param snapshotMgr Snapshot manager.
     */
    public SnapshotRestoreContext(UUID reqId, String snpName, Set<UUID> reqNodes, Collection<String> grps,
        IgniteSnapshotManager snapshotMgr) {
        for (String grpName : grps) {
            cacheIds.add(CU.cacheId(grpName));
            pendingStartCaches.put(grpName, new PendingStartCacheGroup());
        }

        this.reqId = reqId;
        this.reqNodes = reqNodes;
        this.snpName = snpName;
        this.snapshotMgr = snapshotMgr;
    }

    /** @return Request ID. */
    public UUID requestId() {
        return reqId;
    }

    public Set<UUID> nodes() {
        return reqNodes;
    }

    public String snapshotName() {
        return snpName;
    }

    public Set<String> groups() {
        return pendingStartCaches.keySet();
    }

    public void addSharedCache(String cacheName, String grpName) {
        cacheIds.add(CU.cacheId(cacheName));

        PendingStartCacheGroup sharedGrp = pendingStartCaches.get(grpName);

        assert sharedGrp != null : grpName;

        sharedGrp.caches.add(cacheName);
    }

    public boolean containsCache(String name) {
        return cacheIds.contains(CU.cacheId(name));
    }

    public void startConfigs(Collection<StoredCacheData> ccfgs) {
        cacheCfgsToStart = ccfgs;
    }

    public Collection<StoredCacheData> startConfigs() {
        return cacheCfgsToStart;
    }

    public void processCacheStart(String cacheName, @Nullable String grpName, @Nullable Throwable err, ExecutorService svc, GridFutureAdapter<Void> finishFut) {
        String grpName0 = grpName != null ? grpName : cacheName;

        PendingStartCacheGroup pendingGrp = pendingStartCaches.get(grpName0);

        // If any of shared caches has been started - we cannot rollback changes.
        if (pendingGrp.caches.remove(cacheName) && err == null)
            pendingGrp.canRollback = false;

        if (!pendingGrp.caches.isEmpty())
            return;

        if (err != null && pendingGrp.canRollback) {
            svc.submit(() -> {
                rollbackLock.lock();

                try {
                    pendingStartCaches.remove(grpName0);

                    rollback(grpName0);

                    if (pendingStartCaches.isEmpty())
                        finishFut.onDone(err);
                }
                finally {
                    rollbackLock.unlock();
                }
            });

            return;
        }

        rollbackLock.lock();

        try {
            pendingStartCaches.remove(grpName0);

            if (pendingStartCaches.isEmpty())
                finishFut.onDone();
        } finally {
            rollbackLock.unlock();
        }
    }

    /**
     * @param updateMetadata Update binary metadata flag.
     * @param interruptClosure A closure to quickly interrupt the merge process.
     * @throws IgniteCheckedException If failed.
     */
    public void restore(boolean updateMetadata, Supplier<Boolean> interruptClosure) throws IgniteCheckedException {
        if (interruptClosure.get())
            return;

        if (updateMetadata)
            snapshotMgr.mergeSnapshotMetadata(snpName, false, true, interruptClosure);

        for (String grpName : groups()) {
            rollbackLock.lock();

            try {
                if (interruptClosure.get())
                    return;

                List<File> newFiles = new ArrayList<>();

                newGrpFiles.put(grpName, newFiles);

                snapshotMgr.restoreCacheGroupFiles(snpName, grpName, newFiles);
            }
            finally {
                rollbackLock.unlock();
            }
        }
    }

    /**
     * Rollback changes made by process.
     *
     * @param grpName Cache group name.
     */
    public void rollback(String grpName) {
        rollbackLock.lock();

        try {
            List<File> createdFiles = newGrpFiles.remove(grpName);

            if (F.isEmpty(createdFiles))
                return;

            snapshotMgr.rollbackRestoreOperation(createdFiles);
        } finally {
            rollbackLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRestoreContext.class, this);
    }

    /** */
    private static class PendingStartCacheGroup {
        volatile boolean canRollback = true;

        Set<String> caches = new GridConcurrentHashSet<>();
    }
}
