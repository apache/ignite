/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Work batch is an abstraction of the logically grouped tasks.
 */
public class GridGgfsFileWorkerBatch {
    /** Completion latch. */
    private final CountDownLatch completeLatch = new CountDownLatch(1);

    /** Finish guard. */
    private final AtomicBoolean finishGuard = new AtomicBoolean();

    /** Lock for finish operation. */
    private final ReadWriteLock finishLock = new ReentrantReadWriteLock();

    /** Tasks queue. */
    private final BlockingDeque<GridGgfsFileWorkerTask> queue = new LinkedBlockingDeque<>();

    /** Path to the file in the primary file system. */
    private final GridGgfsPath path;

    /** Output stream to the file. */
    private final GridGgfsWriter out;

    /** Caught exception. */
    private volatile GridException err;

    /** Last task marker. */
    private boolean lastTask;

    /**
     * Constructor.
     *
     * @param path Path to the file in the primary file system.
     * @param out Output stream opened to that file.
     */
    GridGgfsFileWorkerBatch(GridGgfsPath path, GridGgfsWriter out) {
        assert path != null;
        assert out != null;

        this.path = path;
        this.out = out;
    }

    /**
     * Perform write.
     *
     * @param data Data to be written.
     * @return {@code True} in case operation was enqueued.
     */
    boolean write(final byte[] data) {
        return addTask(new GridGgfsFileWorkerTask() {
            @Override public void execute() throws GridException {
                out.write(data);
            }
        });
    }

    /**
     * Process the batch.
     */
    void process() {
        try {
            boolean cancelled = false;

            while (!cancelled) {
                try {
                    GridGgfsFileWorkerTask task = queue.poll(1000, TimeUnit.MILLISECONDS);

                    if (task == null)
                        continue;

                    task.execute();

                    if (lastTask)
                        cancelled = true;
                }
                catch (GridException e) {
                    err = e;

                    cancelled = true;
                }
                catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();

                    cancelled = true;
                }
            }
        }
        finally {
            try {
                onComplete();
            }
            finally {
                U.closeQuiet(out);

                completeLatch.countDown();
            }
        }
    }

    /**
     * Add the last task to that batch which will release all the resources.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    void finish() {
        if (finishGuard.compareAndSet(false, true)) {
            finishLock.writeLock().lock();

            try {
                queue.add(new GridGgfsFileWorkerTask() {
                    @Override public void execute() {
                        assert queue.isEmpty();

                        lastTask = true;
                    }
                });
            }
            finally {
                finishLock.writeLock().unlock();
            }
        }
    }

    /**
     * Await for that worker batch to complete.
     *
     * @throws GridException In case any exception has occurred during batch tasks processing.
     */
    void await() throws GridException {
        try {
            completeLatch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new GridInterruptedException(e);
        }

        GridException err0 = err;

        if (err0 != null)
            throw err0;
    }

    /**
     * Await for that worker batch to complete in case it was marked as finished.
     *
     * @throws GridException In case any exception has occurred during batch tasks processing.
     */
    void awaitIfFinished() throws GridException {
        if (finishGuard.get())
            await();
    }

    /**
     * Get primary file system path.
     *
     * @return Primary file system path.
     */
    GridGgfsPath path() {
        return path;
    }

    /**
     * Callback invoked when all the tasks within the batch are completed.
     */
    protected void onComplete() {
        // No-op.
    }

    /**
     * Add task to the queue.
     *
     * @param task Task to add.
     * @return {@code True} in case the task was added to the queue.
     */
    private boolean addTask(GridGgfsFileWorkerTask task) {
        finishLock.readLock().lock();

        try {
            if (!finishGuard.get()) {
                try {
                    queue.put(task);

                    return true;
                }
                catch (InterruptedException ignore) {
                    // Task was not enqueued due to interruption.
                    Thread.currentThread().interrupt();

                    return false;
                }
            }
            else
                return false;

        }
        finally {
            finishLock.readLock().unlock();
        }
    }
}
