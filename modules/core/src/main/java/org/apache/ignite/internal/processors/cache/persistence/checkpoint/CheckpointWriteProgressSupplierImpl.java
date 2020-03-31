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

import org.apache.ignite.internal.util.typedef.internal.A;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provider implementation for counters of checkpoint writes progress.
 */
public class CheckpointWriteProgressSupplierImpl implements CheckpointWriteProgressSupplier {
    /** Counter for written checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger writtenPagesCntr;

    /** Counter for fsynced checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger syncedPagesCntr;

    /** Counter for evicted checkpoint pages. Not null only if checkpoint is running. */
    private volatile AtomicInteger evictedPagesCntr;

    /** Number of pages in current checkpoint at the beginning of checkpoint. */
    private volatile int currCheckpointPagesCnt;

    /** {@inheritDoc} */
    @Override public AtomicInteger writtenPagesCounter() {
        return writtenPagesCntr;
    }

    /**
     * Update written pages in checkpoint;
     *
     * @param deltha Pages num to update.
     */
    public void updateWrittenPagesCounter(int deltha) {
        A.ensure(deltha > 0, "param must be positive");

        writtenPagesCntr.addAndGet(deltha);
    }

    /** {@inheritDoc} */
    @Override public AtomicInteger syncedPagesCounter() {
        return syncedPagesCntr;
    }

    /**
     * Update synced pages in checkpoint;
     *
     * @param deltha Pages num to update.
     */
    public void updateSyncedPages(int deltha) {
        A.ensure(deltha > 0, "param must be positive");

        syncedPagesCntr.addAndGet(deltha);
    }

    /** {@inheritDoc} */
    @Override public AtomicInteger evictedPagesCntr() {
        return evictedPagesCntr;
    }

    /**
     * Update evicted pages in checkpoint;
     *
     * @param deltha Pages num to update.
     */
    public void updateEvictedPagesCntr(int deltha) {
        A.ensure(deltha > 0, "param must be positive");

        if (evictedPagesCntr() != null)
            evictedPagesCntr().addAndGet(deltha);
    }

    /** {@inheritDoc} */
    @Override public int currentCheckpointPagesCount() {
        return currCheckpointPagesCnt;
    }

    /**
     * Sets current checkpoint pages to store.
     *
     * @param num Pages to store.
     */
    public void currentCheckpointPagesCount(int num) {
        currCheckpointPagesCnt = num;
    }

    /** Clears all counters. */
    public void clearCounters() {
        currCheckpointPagesCnt = 0;

        writtenPagesCntr = null;
        syncedPagesCntr = null;
        evictedPagesCntr = null;
    }

    /** Initialize all counters before checkpoint.  */
    public void initCounters(int pagesSize) {
        currCheckpointPagesCnt = pagesSize;

        writtenPagesCntr = new AtomicInteger();
        syncedPagesCntr = new AtomicInteger();
        evictedPagesCntr = new AtomicInteger();
    }
}
