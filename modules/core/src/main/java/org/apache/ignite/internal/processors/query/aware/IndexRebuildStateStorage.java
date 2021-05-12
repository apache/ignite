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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
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
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 * Holder of up-to-date information about rebuilding cache indexes.
 * Helps to avoid the situation when the index rebuilding process is interrupted
 * and after a node restart/reactivation the indexes will become inconsistent.
 * <p/>
 * To do this, before rebuilding the indexes, call {@link #onStartRebuildIndexes}
 * and after it {@link #onFinishRebuildIndexes}. Use {@link #completed} to check
 * if the index rebuild has completed.
 * <p/>
 * To prevent leaks, it is necessary to use {@link #completeRebuildIndexes}
 * when detecting the fact of destroying the cache.
 */
public class IndexRebuildStateStorage implements MetastorageLifecycleListener, CheckpointListener {
    /** Key prefix for the MetaStorage. */
    public static final String KEY_PREFIX = "rebuild-sql-indexes-";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** MetaStorage synchronization mutex. */
    private final Object metaStorageMux = new Object();

    /** Lock. */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Stopping the grid.
     * Guarded by {@link #lock}.
     */
    private boolean stopGrid;

    /**
     * Current states.
     * Mapping: cache name -> index rebuild state.
     * Guarded by {@link #lock}.
     */
    private final Map<String, IndexRebuildState> states = new HashMap<>();

    /**
     * Persistent caches for which the indexes have been rebuilt,
     * and need to delete an entry about this from the MetaStorage.
     * Guarded by {@link #lock}.
     */
    private final Set<String> toRmv = new HashSet<>();

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
    public void onQueryStart() {
        ctx.internalSubscriptionProcessor().registerMetastorageListener(this);
    }

    /**
     * Callback on start of {@link GridCacheProcessor}.
     */
    public void onCacheKernalStart() {
        lock.lock();

        try {
            if (!states.isEmpty()) {
                Set<String> toComplete = new HashSet<>(states.keySet());
                toComplete.removeAll(ctx.cache().cacheDescriptors().keySet());

                for (String cacheName : toComplete)
                    completeRebuildIndexes(cacheName);
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Callback on kernel stop.
     */
    public void onKernalStop() {
        lock.lock();

        try {
            stopGrid = true;
        }
        finally {
            lock.unlock();
        }
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
        lock.lock();

        try {
            assert !stopGrid;

            String cacheName = cacheCtx.name();
            boolean persistent = CU.isPersistentCache(cacheCtx.config(), ctx.config().getDataStorageConfiguration());

            states.put(cacheName, new IndexRebuildState(persistent));

            if (persistent) {
                toRmv.remove(cacheName);

                metaStorageOperation(metaStorage -> {
                    assert metaStorage != null;

                    metaStorage.write(metaStorageKey(cacheName), new IndexRebuildCacheInfo(cacheName));
                });
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Callback on finish of rebuild cache indexes.
     * {@link #onStartRebuildIndexes} is expected to have been called before.
     * <p/>
     * If the cache is persistent, then we mark that the rebuilding of the
     * indexes is completed and the entry will be deleted from the MetaStorage
     * at the end of the checkpoint. Otherwise, delete the index rebuild entry.
     *
     * @param cacheCtx Cache context.
     * @see #onStartRebuildIndexes
     */
    public void onFinishRebuildIndexes(GridCacheContext cacheCtx) {
        lock.lock();

        try {
            String cacheName = cacheCtx.name();

            IndexRebuildState state = states.get(cacheName);

            assert state != null : cacheName;

            complete(cacheName, state);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Force a mark that the index rebuild for the cache has completed.
     * {@link #onStartRebuildIndexes} is not expected to have been called before.
     * <p/>
     * If the cache is persistent, then we mark that the rebuilding of the
     * indexes is completed and the entry will be deleted from the MetaStorage
     * at the end of the checkpoint. Otherwise, delete the index rebuild entry.
     *
     * @param cacheName Cache name.
     */
    public void completeRebuildIndexes(String cacheName) {
        lock.lock();

        try {
            IndexRebuildState state = states.get(cacheName);

            if (state != null)
                complete(cacheName, state);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Check if rebuilding of indexes for the cache has been completed.
     *
     * @param cacheName Cache name.
     * @return {@code True} if completed.
     */
    public boolean completed(String cacheName) {
        lock.lock();

        try {
            IndexRebuildState state = states.get(cacheName);

            return state == null || state.completed();
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) {
        ((GridCacheDatabaseSharedManager)ctx.cache().context().database()).addCheckpointListener(this);
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) {
        lock.lock();

        try {
            assert !stopGrid;

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
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) {
        lock.lock();

        try {
            if (!stopGrid) {
                for (Iterator<Entry<String, IndexRebuildState>> it = states.entrySet().iterator(); it.hasNext(); ) {
                    Entry<String, IndexRebuildState> e = it.next();

                    IndexRebuildState state = e.getValue();

                    if (state.completed()) {
                        assert state.persistent();

                        toRmv.add(e.getKey());

                        it.remove();
                    }
                }
            }
        }
        finally {
            lock.unlock();
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
        lock.lock();

        try {
            if (!stopGrid) {
                for (Iterator<String> it = toRmv.iterator(); it.hasNext(); ) {
                    String cacheName = it.next();

                    metaStorageOperation(metaStorage -> {
                        assert metaStorage != null;

                        metaStorage.remove(metaStorageKey(cacheName));

                        it.remove();
                    });
                }
            }
        }
        finally {
            lock.unlock();
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
     * If the cache is persist, then it sets the status to completed in order
     * to then delete the entry from the MetaStorage at the end of the checkpoint,
     * otherwise it deletes it from the {@link #states}.
     *
     * @param cacheName Cache name.
     * @param state State rebuild indexes of cache.
     */
    private void complete(String cacheName, IndexRebuildState state) {
        assert lock.isHeldByCurrentThread();

        if (state.persistent())
            state.completed(true);
        else
            states.remove(cacheName);
    }
}
