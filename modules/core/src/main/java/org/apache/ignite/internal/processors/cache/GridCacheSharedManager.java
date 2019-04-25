/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Cache manager shared across all caches.
 */
public interface GridCacheSharedManager<K, V> {
    /**
     * Starts manager.
     *
     * @param cctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void start(GridCacheSharedContext<K, V> cctx) throws IgniteCheckedException;

    /**
     * Stops manager.
     *
     * @param cancel Cancel flag.
     */
    public void stop(boolean cancel);

    /**
     * @param cancel Cancel flag.
     */
    public void onKernalStop(boolean cancel);

    /**
     * @param reconnectFut Reconnect future.
     */
    public void onDisconnected(IgniteFuture<?> reconnectFut);

    /**
     * @param active Active flag.
     */
    public void onReconnected(boolean active);

    /**
     * Prints memory statistics (sizes of internal data structures, etc.).
     *
     * NOTE: this method is for testing and profiling purposes only.
     */
    public void printMemoryStats();
}