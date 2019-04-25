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
import org.apache.ignite.IgniteLock;

/**
 * Grid cache reentrant lock ({@code 'Ex'} stands for external).
 */
public interface GridCacheLockEx extends IgniteLock, GridCacheRemovable {
    /**
     * Get current reentrant lock latch key.
     *
     * @return Lock key.
     */
    public GridCacheInternalKey key();

    /**
     * Callback to notify reentrant lock on changes.
     *
     * @param state New reentrant lock state.
     */
    public void onUpdate(GridCacheLockState state);

    /**
     * Callback to notify semaphore on topology changes.
     *
     * @param nodeId Id of the node that left the grid.
     */
    public void onNodeRemoved(UUID nodeId);

    /**
     * Callback to notify local reentrant lock instance on node stop.
     */
    public void onStop();
}
