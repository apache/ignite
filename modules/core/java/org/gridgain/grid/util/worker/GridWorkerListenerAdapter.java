/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.worker;

/**
 * Convenience adapter for {@link GridWorkerListener}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridWorkerListenerAdapter implements GridWorkerListener {
    /** {@inheritDoc} */
    @Override public void onStarted(GridWorker w) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public void onStopped(GridWorker w) {
        /* No-op. */
    }
}
