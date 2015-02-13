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

package org.apache.ignite.internal.processors.fs;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * GGFS file worker for DUAL modes.
 */
public class IgfsFileWorker extends IgfsThread {
    /** Time during which thread remains alive since it's last batch is finished. */
    private static final long THREAD_REUSE_WAIT_TIME = 5000;

    /** Lock */
    private final Lock lock = new ReentrantLock();

    /** Condition. */
    private final Condition cond = lock.newCondition();

    /** Next queued batch. */
    private GridGgfsFileWorkerBatch nextBatch;

    /** Batch which is currently being processed. */
    private GridGgfsFileWorkerBatch curBatch;

    /** Cancellation flag. */
    private volatile boolean cancelled;

    /**
     * Creates {@code GGFS} file worker.
     *
     * @param name Worker name.
     */
    IgfsFileWorker(String name) {
        super(name);
    }

    /**
     * Add worker batch.
     *
     * @return {@code True} if the batch was actually added.
     */
    boolean addBatch(GridGgfsFileWorkerBatch batch) {
        assert batch != null;

        lock.lock();

        try {
            if (!cancelled) {
                assert nextBatch == null; // Remember, that write operations on a single file are exclusive.

                nextBatch = batch;

                cond.signalAll();

                return true;
            }
            else
                return false;
        }
        finally {
            lock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException {
        while (!cancelled) {
            lock.lock();

            try {
                // If there are no more new batches, wait for several seconds before shutting down the thread.
                if (!cancelled && nextBatch == null)
                    cond.await(THREAD_REUSE_WAIT_TIME, TimeUnit.MILLISECONDS);

                curBatch = nextBatch;

                nextBatch = null;

                if (cancelled && curBatch != null)
                    curBatch.finish(); // Mark the batch as finished if cancelled.
            }
            finally {
                lock.unlock();
            }

            if (curBatch != null)
                curBatch.process();
            else {
                lock.lock();

                try {
                    // No more new batches, we can safely release the worker as it was inactive for too long.
                    if (nextBatch == null)
                        cancelled = true;
                }
                finally {
                    lock.unlock();
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void cleanup() {
        assert cancelled; // Cleanup can only be performed on a cancelled worker.

        // Clear interrupted flag.
        boolean interrupted = interrupted();

        // Process the last batch if any.
        if (nextBatch != null)
            nextBatch.process();

        onFinish();

        // Reset interrupted flag.
        if (interrupted)
            interrupt();
    }

    /**
     * Forcefully finish execution of all batches.
     */
    void cancel() {
        lock.lock();

        try {
            cancelled = true;

            if (curBatch != null)
                curBatch.finish();

            if (nextBatch != null)
                nextBatch.finish();

            cond.signalAll(); // Awake the main loop in case it is still waiting for the next batch.
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Get current batch.
     *
     * @return Current batch.
     */
    GridGgfsFileWorkerBatch currentBatch() {
        lock.lock();

        try {
            return nextBatch == null ? curBatch : nextBatch;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Callback invoked when worker has processed all it's batches.
     */
    protected void onFinish() {
        // No-op.
    }
}
