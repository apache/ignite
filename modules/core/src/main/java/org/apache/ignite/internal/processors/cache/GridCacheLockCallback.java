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

import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;

/**
 * Lock and Unlock callbacks.
 */
public interface GridCacheLockCallback {
    /**
     * Called when entry gets a first candidate. This call
     * happens within entry internal synchronization.
     *
     * @param entry Entry.
     */
    public void onLocked(GridDistributedCacheEntry entry);

    /**
     * Called when entry lock ownership changes. This call
     * happens outside of synchronization so external callbacks
     * can be made from this call.
     *
     * @param entry Entry.
     * @param owner Current owner.
     */
    public void onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner);

    /**
     * Called when entry has no more candidates. This call happens
     * within entry internal synchronization.
     *
     * @param entry Entry
     */
    public void onFreed(GridDistributedCacheEntry entry);
}