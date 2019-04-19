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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provider for counters of checkpoint writes progress.
 */
public interface CheckpointWriteProgressSupplier {
    /**
     * @return Counter for written checkpoint pages. Not <code>null</code> only if checkpoint is running.
     */
    public AtomicInteger writtenPagesCounter();

    /**
     * @return Counter for fsynced checkpoint pages. Not  <code>null</code> only if checkpoint is running.
     */
    public AtomicInteger syncedPagesCounter();

    /**
     * @return Counter for evicted pages during current checkpoint. Not <code>null</code> only if checkpoint is running.
     */
    public AtomicInteger evictedPagesCntr();

    /**
     * @return Number of pages in current checkpoint. If checkpoint is not running, returns 0.
     */
    public int currentCheckpointPagesCount();
}
