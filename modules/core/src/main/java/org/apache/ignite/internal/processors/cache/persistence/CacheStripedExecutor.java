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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_RECOVERY_SEMAPHORE_PERMITS;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointReadWriteLock.CHECKPOINT_LOCK_HOLD_COUNT;

/**
 * Wrapper over {@link StripedExecutor}. Cache operations are grouped by IgniteCache group and partition.
 */
public class CacheStripedExecutor {
    /** It is used for CAS updates of {@link #error}. */
    private static final AtomicReferenceFieldUpdater<CacheStripedExecutor, IgniteCheckedException> ERROR =
        AtomicReferenceFieldUpdater.newUpdater(CacheStripedExecutor.class, IgniteCheckedException.class, "error");

    /** Error appeared during submitted task execution. */
    private volatile IgniteCheckedException error;

    /** Delegate executor. */
    private final StripedExecutor exec;

    /** Limit number of concurrent tasks submitted to the executor. Helps to avoid OOM error. */
    private final Semaphore semaphore;

    /** */
    public CacheStripedExecutor(StripedExecutor exec) {
        this.exec = exec;

        semaphore = new Semaphore(semaphorePermits(exec));
    }

    /**
     * Submit task to striped executor.
     *
     * @param task Runnable task.
     * @param grpId Group ID.
     * @param partId Partition ID.
     */
    public void submit(Runnable task, int grpId, int partId) {
        int stripes = exec.stripesCount();

        int stripe = U.stripeIdx(stripes, grpId, partId);

        assert stripe >= 0 && stripe <= stripes : "idx=" + stripe + ", stripes=" + stripes;

        try {
            semaphore.acquire();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }

        exec.execute(stripe, () -> {
            // WA for avoid assert check in PageMemory, that current thread hold chpLock.
            CHECKPOINT_LOCK_HOLD_COUNT.set(1);

            try {
                task.run();
            }
            finally {
                CHECKPOINT_LOCK_HOLD_COUNT.set(0);

                semaphore.release();
            }
        });
    }

    /**
     * Awaits while all submitted tasks completed.
     *
     * @throws IgniteCheckedException if any os submitted tasks failed.
     */
    public void awaitApplyComplete() throws IgniteCheckedException {
        try {
            // Await completion apply tasks in all stripes.
            exec.awaitComplete();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }

        // Checking error after all task applied.
        IgniteCheckedException error = ERROR.get(this);

        if (error != null)
            throw error;
    }

    /**
     * @return {@code true} if any of submitted tasks failed.
     */
    public boolean error() {
        return ERROR.get(this) != null;
    }

    /**
     * @param e Error appeared during submitted task execution.
     */
    public void onError(IgniteCheckedException e) {
        ERROR.compareAndSet(this, null, e);
    }

    /**
     * Calculate the maximum number of concurrent tasks for apply through the striped executor.
     *
     * @param exec Striped executor.
     * @return Number of permits.
     */
    private int semaphorePermits(StripedExecutor exec) {
        // 4 task per-stripe by default.
        int permits = exec.stripesCount() * 4;

        long maxMemory = Runtime.getRuntime().maxMemory();

        // Heuristic calculation part of heap size as a maximum number of concurrent tasks.
        int permits0 = (int)((maxMemory * 0.2) / (4096 * 2));

        // May be for small heap. Get a low number of permits.
        if (permits0 < permits)
            permits = permits0;

        // Property for override any calculation.
        return getInteger(IGNITE_RECOVERY_SEMAPHORE_PERMITS, permits);
    }
}
