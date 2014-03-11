/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import java.util.concurrent.*;

/**
 * Future for asynchronous operations.
 */
public interface GridClientFuture<R> {
    /**
     * Synchronously waits for completion and returns result.
     *
     * @return Completed future result.
     * @throws GridClientException In case of error.
     */
    public R get() throws GridClientException;

    /**
     * Synchronously waits for completion and returns result.
     *
     * @param timeout Timeout interval to wait future completes.
     * @param unit Timeout interval unit to wait future completes.
     * @return Completed future result.
     * @throws GridClientException In case of error.
     * @throws GridClientFutureTimeoutException If timed out before future finishes.
     */
    public R get(long timeout, TimeUnit unit) throws GridClientException;

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
    public void listenAsync(GridClientFutureListener<R>... lsnrs);

    /**
     * Removes listeners registered before.
     *
     * @param lsnrs Listeners to be removed.
     */
    public void stopListenAsync(GridClientFutureListener<R>... lsnrs);
}
