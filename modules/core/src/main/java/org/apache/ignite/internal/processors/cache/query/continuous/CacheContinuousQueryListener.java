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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.Map;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Continuous query listener.
 */
public interface CacheContinuousQueryListener<K, V> {
    /**
     * Entry update callback.
     *
     * @param evt Event
     * @param primary Primary flag.
     * @param recordIgniteEvt Whether to record event.
     * @param fut Dht atomic future.
     */
    public void onEntryUpdated(CacheContinuousQueryEvent<K, V> evt, boolean primary,
        boolean recordIgniteEvt, @Nullable GridDhtAtomicAbstractUpdateFuture fut);

    /**
     *
     */
    public void onBeforeRegister();

    /**
     *
     */
    public void onAfterRegister();

    /**
     * Listener registration callback.
     * NOTE: This method should be called under the {@link CacheGroupContext#listenerLock()}} write lock held.
     */
    public void onRegister();

    /**
     * Listener unregistered callback.
     */
    public void onUnregister();

    /**
     * Cleans backup queue.
     *
     * @param updateCntrs Update indexes map.
     */
    public void cleanupBackupQueue(Map<Integer, Long> updateCntrs);

    /**
     * Flushes backup queue.
     *
     * @param ctx Context.
     * @param topVer Topology version.
     */
    public void flushBackupQueue(GridKernalContext ctx, AffinityTopologyVersion topVer);

    /**
     * @param ctx Context.
     */
    public void acknowledgeBackupOnTimeout(GridKernalContext ctx);

    /**
     * @param evt Event
     * @param topVer Topology version.
     * @param primary Primary
     */
    public void skipUpdateEvent(CacheContinuousQueryEvent<K, V> evt, AffinityTopologyVersion topVer, boolean primary);

    /**
     * For cache updates in shared cache group need notify others caches CQ listeners
     * that generated counter should be skipped.
     *
     * @param cctx Cache context.
     * @param skipCtx Context.
     * @param part Partition.
     * @param cntr Counter to skip.
     * @param topVer Topology version.
     * @return Context.
     */
    @Nullable public CounterSkipContext skipUpdateCounter(
        GridCacheContext cctx,
        @Nullable CounterSkipContext skipCtx,
        int part,
        long cntr,
        AffinityTopologyVersion topVer,
        boolean primary);

    /**
     * @param part Partition.
     */
    public void onPartitionEvicted(int part);

    /**
     * @return Whether old value is required.
     */
    public boolean oldValueRequired();

    /**
     * @return Keep binary flag.
     */
    public boolean keepBinary();

    /**
     * @return Whether to notify on existing entries.
     */
    public boolean notifyExisting();

    /**
     * @return {@code True} if this listener should be called on events on primary partitions only.
     */
    public boolean isPrimaryOnly();
}
