// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.lang;

import org.gridgain.grid.*;

/**
 * TODO: Add interface description.
 */
public interface IgniteAsyncSupport {
    /**
     * Gets component with asynchronous mode enabled.
     *
     * @return Component with asynchronous mode enabled.
     */
    public IgniteAsyncSupport enableAsync();

    /**
     * @return {@code True} if asynchronous mode is enabled.
     */
    public boolean isAsync();

    /**
     * Gets and resets future for previous asynchronous operation.
     *
     * @return Future for previous asynchronous operation.
     */
    public <R> GridFuture<R> future();
}
