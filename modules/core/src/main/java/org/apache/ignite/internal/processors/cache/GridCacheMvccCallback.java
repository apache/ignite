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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;

/**
 * Lock and Unlock callbacks.
 */
public interface GridCacheMvccCallback {
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
     * @param prev Previous candidate.
     * @param owner Current owner.
     */
    public void onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate prev,
        GridCacheMvccCandidate owner);

    /**
     * Called when entry has no more candidates. This call happens
     * within entry internal synchronization.
     *
     * @param entry Entry
     */
    public void onFreed(GridDistributedCacheEntry entry);
}