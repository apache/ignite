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

import static org.apache.ignite.internal.processors.query.aware.IndexRebuildState.State.COMPLETED;
import static org.apache.ignite.internal.processors.query.aware.IndexRebuildState.State.DELETE;
import static org.apache.ignite.internal.processors.query.aware.IndexRebuildState.State.INIT;

/**
 * Holder of up-to-date information about rebuilding cache indexes.
 * Helps to avoid the situation when the index rebuilding process is interrupted
 * and after a node restart/reactivation the indexes will become inconsistent.
 * <p/>
 * To do this, before rebuilding the indexes, call {@link #onStartRebuildIndexes}
 * and after it {@link #onFinishRebuildIndexes}. Use {@link #completed} to check
 * if the index rebuild has completed.
 * <p/>
 * To prevent leaks, it is necessary to use {@link #onFinishRebuildIndexes}
 * when detecting the fact of destroying the cache.
 */
public class IndexRebuildStateStorage implements MetastorageLifecycleListener, CheckpointListener {
    /** Key prefix for the MetaStorage. */
    public static final String KEY_PREFIX = "rebuild-sql-indexes-";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** MetaStorage synchronization mutex. */
    private final Object metaStorageMux = new Object();

    /** Node stop lock. */
    private final GridBusyLock stopNodeLock = new GridBusyLock();

    /** Current states. Mapping: cache name -> index rebuild state. */
    private final ConcurrentMap<String, IndexRebuildState> states = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public IndexRebuildStateStorage(GridKernalContext ctx) {
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
        Set<String> toComplete = new HashSet<>(states.keySet());
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
     * Callback on start of rebuild cache indexes.
     * <p/>
     * Adding an entry that rebuilding the cache indexes in progress.
     * If the cache is persistent, then add this entry to the MetaStorage.
     *
     * @param cacheCtx Cache context.
     * @see #onFinishRebuildIndexes
     */
    public void onStartRebuildIndexes(GridCacheContext cacheCtx) {
        if (!stopNodeLock.enterBusy())
            throw new IgniteException("Node is stopping.");

        try {
            String cacheName = cacheCtx.name();
            boolean persistent = CU.isPersistentCache(cacheCtx.config(), ctx.config().getDataStorageConfiguration());

            states.compute(cacheName, (k, prev) -> {
                if (prev != null) {
                    prev.state(INIT);

                    return prev;
                }
                else
                    return new IndexRebuildState(persistent);
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
     * Callback on finish of rebuild cache indexes.
     * <p/>
     * If the cache is persistent, then we mark that the rebuilding of the
     * indexes is completed and the entry will be deleted from the MetaStorage
     * at the end of the checkpoint. Otherwise, delete the index rebuild entry.
     *
     * @param cacheName Cache name.
     * @see #onStartRebuildIndexes
     */
    public void onFinishRebuildIndexes(String cacheName) {
        states.compute(cacheName, (k, prev) -> {
            if (prev != null && prev.persistent()) {
                prev.state(INIT, COMPLETED);

                return prev;
            }
            else
                return null;
        });
    }

    /**
     * Check if rebuilding of indexes for the cache has been completed.
     *
     * @param cacheName Cache name.
     * @return {@code True} if completed.
     */
    public boolean completed(String cacheName) {
        IndexRebuildState state = states.get(cacheName);

        return state == null || state.state() != INIT;
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

                        states.put(cacheInfo.cacheName(), new IndexRebuildState(true));
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
            for (IndexRebuildState state : states.values()) {
                if (state.state(COMPLETED, DELETE))
                    assert state.persistent();
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
            for (String cacheName : states.keySet()) {
                // Trying to concurrently delete the state.
                IndexRebuildState newVal =
                    states.compute(cacheName, (k, prev) -> prev != null && prev.state() == DELETE ? null : prev);

                // Assume that the state has been deleted.
                if (newVal == null) {
                    metaStorageOperation(metaStorage -> {
                        assert metaStorage != null;

                        if (!states.containsKey(cacheName))
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
}
