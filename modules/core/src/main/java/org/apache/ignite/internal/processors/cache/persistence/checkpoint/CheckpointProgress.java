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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.NotNull;

/**
 * Represents information of a progress of a given checkpoint and
 * allows to obtain future to wait for a particular checkpoint state.
 */
public interface CheckpointProgress {
    /** */
    public boolean inProgress();

    /** */
    public GridFutureAdapter futureFor(CheckpointState state);

    /**
     * Mark this checkpoint execution as failed.
     *
     * @param error Causal error of fail.
     */
    public void fail(Throwable error);

    /**
     * Changing checkpoint state if order of state is correct.
     *
     * @param newState New checkpoint state.
     */
    public void transitTo(@NotNull CheckpointState newState);

    /**
     * @return PartitionDestroyQueue.
     */
    public PartitionDestroyQueue getDestroyQueue();

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
    public AtomicInteger evictedPagesCounter();

    /**
     * @return Number of pages in current checkpoint. If checkpoint is not running, returns 0.
     */
    public int currentCheckpointPagesCount();

    /**
     * Sets current checkpoint pages num to store.
     *
     * @param num Pages to store.
     */
    public void currentCheckpointPagesCount(int num);

    /** Initialize all counters before checkpoint.  */
    public void initCounters(int pagesSize);

    /**
     * Update synced pages in checkpoint;
     *
     * @param deltha Pages num to update.
     */
    public void updateSyncedPages(int deltha);

    /**
     * Update written pages in checkpoint;
     *
     * @param deltha Pages num to update.
     */
    public void updateWrittenPages(int deltha);

    /**
     * Update evicted pages in checkpoint;
     *
     * @param deltha Pages num to update.
     */
    public void updateEvictedPages(int deltha);

    /** Clear cp progress counters. */
    public void clearCounters();
}
