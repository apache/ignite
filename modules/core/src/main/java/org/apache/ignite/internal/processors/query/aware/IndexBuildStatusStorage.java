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

package org.apache.ignite.internal.processors.query.aware;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.internal.processors.query.aware.IndexBuildStatusHolder.Status.COMPLETE;
import static org.apache.ignite.internal.processors.query.aware.IndexBuildStatusHolder.Status.DELETE;

/**
 * Holder of up-to-date information about cache index building operations (rebuilding indexes, building new indexes).
 * Avoids index inconsistency when operations have completed, a checkpoint has not occurred, and the node has been restarted.
 * <p/>
 * For rebuild indexes, use {@link #onStartRebuildIndexes} and {@link #onFinishRebuildIndexes}.
 * For build new indexes, use {@link #onStartBuildNewIndex} and {@link #onFinishBuildNewIndex}.
 * Use {@link #rebuildCompleted} to check that the index rebuild is complete.
 */
public class IndexBuildStatusStorage implements MetastorageLifecycleListener, CheckpointListener {
    /** Key prefix for the MetaStorage. */
    public static final String KEY_PREFIX = "rebuild-sql-indexes-";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** MetaStorage synchronization mutex. */
    private final Object metaStorageMux = new Object();

    /** Node stop lock. */
    private final GridBusyLock stopNodeLock = new GridBusyLock();

    /** Current statuses. Mapping: cache name -> index build status. */
    private final ConcurrentMap<String, IndexBuildStatusHolder> statuses = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public IndexBuildStatusStorage(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Callback on start of {@link GridQueryProcessor}.
     */
    public void start() {
        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    /**
     * Callback on start of {@link GridCacheProcessor}.
     */
    public void onCacheKernalStart() {
        Set<String> toComplete = new HashSet<>(statuses.keySet());
        toComplete.removeAll(ctx.cache().cacheDescriptors().keySet());

        for (String cacheName : toComplete)
            onFinishRebuildIndexes(cacheName);
    }

    /**
     * Callback on kernel stop.
     */
    public void stop() {
        stopNodeLock.block();
    }

    /**
     * Callback on the start of rebuilding cache indexes.
     * <p/>
     * Registers the start of rebuilding cache indexes, for persistent cache
     * writes a entry to the MetaStorage so that if a failure occurs,
     * the indexes are automatically rebuilt.
     *
     * @param cacheCtx Cache context.
     * @see #onFinishRebuildIndexes
     */
    public void onStartRebuildIndexes(GridCacheContext cacheCtx) {
        onStartOperation(cacheCtx, true);
    }

    /**
     * Callback on the start of building a new cache index.
     * <p/>
     * Registers the start of building a new cache index, for persistent cache
     * writes a entry to the MetaStorage so that if a failure occurs,
     * the indexes are automatically rebuilt.
     *
     * @param cacheCtx Cache context.
     * @see #onFinishBuildNewIndex
     */
    public void onStartBuildNewIndex(GridCacheContext cacheCtx) {
        onStartOperation(cacheCtx, false);
    }

    /**
     * Callback on the finish of rebuilding cache indexes.
     * <p/>
     * Registers the finish of rebuilding cache indexes, if all operations
     * have been completed for persistent cache, then the entry will be deleted
     * from the MetaStorage at the end of the checkpoint,
     * otherwise for the in-memory cache the status will be deleted immediately.
     *
     * @param cacheName Cache name.
     * @see #onStartRebuildIndexes
     */
    public void onFinishRebuildIndexes(String cacheName) {
        onFinishOperation(cacheName, true);
    }

    /**
     * Callback on the finish of building a new cache index.
     * <p/>
     * Registers the finish of building a new cache index, if all operations
     * have been completed for persistent cache, then the entry will be deleted
     * from the MetaStorage at the end of the checkpoint,
     * otherwise for the in-memory cache the status will be deleted immediately.
     *
     * @param cacheName Cache name.
     * @see #onStartBuildNewIndex
     */
    public void onFinishBuildNewIndex(String cacheName) {
        onFinishOperation(cacheName, false);
    }

    /**
     * Check if rebuilding of indexes for the cache has been completed.
     *
     * @param cacheName Cache name.
     * @return {@code True} if completed.
     */
    public boolean rebuildCompleted(String cacheName) {
        IndexBuildStatusHolder status = statuses.get(cacheName);

        return status == null || !status.rebuild();
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) {
        ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).addCheckpointListener(this);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) {
        if (!stopNodeLock.enterBusy())
            return;

        try {
            metaStorageOperation(metaStorage -> {
                assert metaStorage != null;

                metaStorage.iterate(
                    KEY_PREFIX,
                    (k, v) -> {
                        IndexRebuildCacheInfo cacheInfo = (IndexRebuildCacheInfo)v;

                        statuses.put(cacheInfo.cacheName(), new IndexBuildStatusHolder(true, true));
                    },
                    true
                );
            });
        }
        finally {
            stopNodeLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        if (!stopNodeLock.enterBusy())
            return;

        try {
            for (IndexBuildStatusHolder status : statuses.values()) {
                if (status.delete())
                    assert status.persistent();
            }
        }
        finally {
            stopNodeLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void afterCheckpointEnd(Context ctx) {
        if (!stopNodeLock.enterBusy())
            return;

        try {
            for (String cacheName : statuses.keySet()) {
                // Trying to concurrently delete the state.
                IndexBuildStatusHolder newVal =
                    statuses.compute(cacheName, (k, prev) -> prev != null && prev.status() == DELETE ? null : prev);

                // Assume that the state has been deleted.
                if (newVal == null) {
                    metaStorageOperation(metaStorage -> {
                        assert metaStorage != null;

                        if (!statuses.containsKey(cacheName))
                            metaStorage.remove(metaStorageKey(cacheName));
                    });
                }
            }
        }
        finally {
            stopNodeLock.leaveBusy();
        }
    }

    /**
     * Performing an operation on a {@link MetaStorage}.
     * Guarded by {@link #metaStorageMux}.
     *
     * @param consumer MetaStorage operation, argument can be {@code null}.
     * @throws IgniteException If an exception is thrown from the {@code consumer}.
     */
    private void metaStorageOperation(IgniteThrowableConsumer<MetaStorage> consumer) {
        synchronized (metaStorageMux) {
            IgniteCacheDatabaseSharedManager db = ctx.cache().context().database();

            db.checkpointReadLock();

            try {
                consumer.accept(db.metaStorage());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
    }

    /**
     * Getting MetaStorage key for index rebuild state.
     *
     * @param cacheName Cache name.
     * @return MetaStorage key.
     */
    private static String metaStorageKey(String cacheName) {
        return KEY_PREFIX + cacheName;
    }

    /**
     * Callback on the start of the cache index building operation.
     * <p/>
     * Registers the start of an index build operation, for persistent cache
     * writes a entry to the MetaStorage so that if a failure occurs,
     * the indexes are automatically rebuilt.
     *
     * @param cacheCtx Cache context.
     * @param rebuild {@code True} if rebuilding indexes, otherwise building a new index.
     * @see #onFinishOperation
     */
    private void onStartOperation(GridCacheContext cacheCtx, boolean rebuild) {
        if (!stopNodeLock.enterBusy())
            throw new IgniteException("Node is stopping.");

        try {
            String cacheName = cacheCtx.name();
            boolean persistent = CU.isPersistentCache(cacheCtx.config(), ctx.config().getDataStorageConfiguration());

            statuses.compute(cacheName, (k, prev) -> {
                if (prev != null) {
                    prev.onStartOperation(rebuild);

                    return prev;
                }
                else
                    return new IndexBuildStatusHolder(persistent, rebuild);
            });

            if (persistent) {
                metaStorageOperation(metaStorage -> {
                    assert metaStorage != null;

                    metaStorage.write(metaStorageKey(cacheName), new IndexRebuildCacheInfo(cacheName));
                });
            }
        }
        finally {
            stopNodeLock.leaveBusy();
        }
    }

    /**
     * Callback on the finish of the cache index building operation.
     * <p/>
     * Registers the finish of the index build operation, if all operations
     * have been completed for persistent cache, then the entry will be deleted
     * from the MetaStorage at the end of the checkpoint,
     * otherwise for the in-memory cache the status will be deleted immediately.
     *
     * @param cacheName Cache name.
     * @param rebuild {@code True} if rebuilding indexes, otherwise building a new index.
     * @see #onStartOperation
     */
    private void onFinishOperation(String cacheName, boolean rebuild) {
        statuses.compute(cacheName, (k, prev) -> {
            if (prev != null && prev.onFinishOperation(rebuild) == COMPLETE && !prev.persistent())
                return null;
            else
                return prev;
        });
    }
}
