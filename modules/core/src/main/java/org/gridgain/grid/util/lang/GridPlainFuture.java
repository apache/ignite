/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.apache.ignite.*;

import java.util.concurrent.*;

/**
 * Future that does not depend on kernal context.
 */
public interface GridPlainFuture<R> {
    /**
     * Synchronously waits for completion and returns result.
     *
     * @return Completed future result.
     * @throws IgniteCheckedException In case of error.
     */
    public R get() throws IgniteCheckedException;

    /**
     * Synchronously waits for completion and returns result.
     *
     * @param timeout Timeout interval to wait future completes.
     * @param unit Timeout interval unit to wait future completes.
     * @return Completed future result.
     * @throws IgniteCheckedException In case of error.
     * @throws org.apache.ignite.lang.IgniteFutureTimeoutException If timed out before future finishes.
     */
    public R get(long timeout, TimeUnit unit) throws IgniteCheckedException;

    /**
     * Checks if future is done.
     *
     * @return Whether future is done.
     */
    public boolean isDone();

    /**
     * Register new listeners for notification when future completes.
     *
     * Note that current implementations are calling listeners in
     * the completing thread.
     *
     * @param lsnrs Listeners to be registered.
     */
    public void listenAsync(GridPlainInClosure<GridPlainFuture<R>>... lsnrs);

    /**
     * Removes listeners registered before.
     *
     * @param lsnrs Listeners to be removed.
     */
    public void stopListenAsync(GridPlainInClosure<GridPlainFuture<R>>... lsnrs);

    /**
     * Creates a future that will be completed after this future is completed. The result of
     * created future is value returned by {@code cb} closure invoked on this future.
     *
     * @param cb Callback closure.
     * @return Chained future.
     */
    public <T> GridPlainFuture<T> chain(GridPlainClosure<GridPlainFuture<R>, T> cb);
}
