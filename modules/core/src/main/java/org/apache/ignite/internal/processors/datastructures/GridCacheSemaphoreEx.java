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
