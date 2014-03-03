/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs.mapreduce;

/**
 * Adapter for {@link GridGgfsJob} with no-op implementation of {@link #cancel()} method.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridGgfsJobAdapter implements GridGgfsJob {
    /** {@inheritDoc} */
    @Override public void cancel() {
        // No-op.
    }
}
