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

package org.apache.ignite.internal.processors.datastructures;

import java.util.UUID;
import org.apache.ignite.IgniteSemaphore;

/**
 * Grid cache semaphore ({@code 'Ex'} stands for external).
 */
public interface GridCacheSemaphoreEx extends IgniteSemaphore, GridCacheRemovable {
    /**
     * Get current semaphore key.
     *
     * @return Semaphore key.
     */
    public GridCacheInternalKey key();

    /**
     * Callback to notify semaphore on changes.
     *
     * @param val State containing the number of available permissions.
     */
    public void onUpdate(GridCacheSemaphoreState val);

    /**
     * Callback to notify semaphore on topology changes.
     *
     * @param nodeId Id of the node that left the grid.
     */
    public void onNodeRemoved(UUID nodeId);

    /**
     * Callback to notify local semaphore instance on node stop.
     */
    public void stop();
}
