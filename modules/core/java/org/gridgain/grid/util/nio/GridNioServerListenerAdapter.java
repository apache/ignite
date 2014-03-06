/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

/**
 * Server listener adapter providing empty methods implementation for rarely used methods.
 */
public abstract class GridNioServerListenerAdapter<T> implements GridNioServerListener<T> {
    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) {
        // No-op.
    }
}
