/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * GGFS file worker for DUAL modes.
 */
public class GridGgfsFileWorker extends GridGgfsThread {
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
    GridGgfsFileWorker(String name) {
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
