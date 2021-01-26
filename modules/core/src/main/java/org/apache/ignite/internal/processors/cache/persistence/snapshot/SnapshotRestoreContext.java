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
 * Cache group restore from snapshot operation context.
 */
class SnapshotRestoreContext {
    /** Request ID. */
    private final UUID reqId;

    /** Snapshot name. */
    private final String snpName;

    /** List of baseline node IDs that must be alive to complete the operation. */
    private final Set<UUID> reqNodes;

    /** Snapshot manager. */
    private final IgniteSnapshotManager snapshotMgr;

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
     * @param grps List of cache group names to restore from the snapshot.
     * @param snapshotMgr Snapshot manager.
     */
    public SnapshotRestoreContext(UUID reqId, String snpName, Set<UUID> reqNodes, Collection<String> grps,
        IgniteSnapshotManager snapshotMgr) {
        for (String grpName : grps)
            this.grps.put(grpName, new GroupRestoreContext());

        this.reqId = reqId;
        this.reqNodes = reqNodes;
        this.snpName = snpName;
        this.snapshotMgr = snapshotMgr;
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

    /** @return List of cache group names to restore from the snapshot. */
    public Set<String> groups() {
        return grps.keySet();
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
     * @param cacheData Stored cache data.
     */
    public void addCacheData(StoredCacheData cacheData) {
        String cacheName = cacheData.config().getName();

        cacheCfgs.put(CU.cacheId(cacheName), cacheData);

        String grpName = cacheData.config().getGroupName();

        if (grpName == null)
            return;

        GroupRestoreContext grpCtx = grps.get(grpName);

        assert grpCtx != null : grpName;

        grpCtx.caches.add(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @param grpName Group name.
     * @param err Exception (if any).
     * @param svc Executor service for asynchronous rollback.
     * @param finishFut A future to be completed when all restored cache groups are started or rolled back.
     */
    public void processCacheStart(
        String cacheName,
        @Nullable String grpName,
        @Nullable Throwable err,
        ExecutorService svc,
        GridFutureAdapter<Void> finishFut
    ) {
        String grpName0 = grpName != null ? grpName : cacheName;

        GroupRestoreContext grp = grps.get(grpName0);

        // If any of shared caches has been started - we cannot rollback changes.
        if (grp.caches.remove(cacheName) && err == null)
            grp.started = true;

        if (!grp.caches.isEmpty()) {
            if (err != null)
                grp.startErr = err;

            return;
        }

        if (err != null && !grp.started) {
            svc.submit(() -> {
                rollbackLock.lock();

                try {
                    rollback(grpName0);

                    if (grps.isEmpty())
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
            if (grps.remove(grpName0) != null && grps.isEmpty())
                finishFut.onDone(null, err == null ? grp.startErr : err);
        } finally {
            rollbackLock.unlock();
        }
    }

    /**
     * Restore specified cache groups from the local snapshot directory.
     *
     * @param updateMetadata Update binary metadata flag.
     * @param interruptClosure A closure to quickly interrupt the process.
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
                GroupRestoreContext grp = grps.get(grpName);

                snapshotMgr.restoreCacheGroupFiles(snpName, grpName, interruptClosure, grp.files);
            }
            finally {
                rollbackLock.unlock();
            }
        }
    }

    /**
     * Rollback changes made by process in specified cache group.
     *
     * @param grpName Cache group name.
     */
    public void rollback(String grpName) {
        rollbackLock.lock();

        try {
            GroupRestoreContext grp = grps.remove(grpName);

            if (grp == null || F.isEmpty(grp.files))
                return;

            snapshotMgr.rollbackRestoreOperation(grp.files);
        } finally {
            rollbackLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRestoreContext.class, this);
    }

    /** */
    private static class GroupRestoreContext {
        /** List of caches of the cache group. */
        final Set<String> caches = new GridConcurrentHashSet<>();

        /** Files created in the cache group folder during a restore operation. */
        final List<File> files = new ArrayList<>();

        /** The flag indicates that one of the caches in this cache group has been started. */
        volatile boolean started;

        /** An exception that was thrown when starting a shared cache group (if any). */
        volatile Throwable startErr;
    }
}
