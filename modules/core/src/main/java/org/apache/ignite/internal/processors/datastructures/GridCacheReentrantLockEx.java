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
import org.apache.ignite.IgniteReentrantLock;

/**
 * Grid cache reentrant lock ({@code 'Ex'} stands for external).
 */
public interface GridCacheReentrantLockEx extends IgniteReentrantLock, GridCacheRemovable {
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
    public void onUpdate(GridCacheReentrantLockState state);

    /**
     * Callback to notify semaphore on topology changes.
     *
     * @param nodeId Id of the node that left the grid.
     */
    public void onNodeRemoved(UUID nodeId);
}
