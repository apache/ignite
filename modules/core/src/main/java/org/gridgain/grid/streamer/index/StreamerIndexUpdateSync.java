/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer.index;

import org.gridgain.grid.util.typedef.internal.*;

/**
 * Streamer index update synchronizer.
 * <p>
 * Used in {@link StreamerIndexProvider} to synchronize
 * operations on window index.
 *
 * @see StreamerIndexProvider
 *
 */
public class StreamerIndexUpdateSync {
    /** */
    private volatile int res;

    /**
     * Waits for a notification from another thread, which
     * should call {@link #finish(int)} with an operation result.
     * That result is returned by this method.
     *
     * @return Operation results, passed to {@link #finish(int)}.
     * @throws InterruptedException If wait was interrupted.
     */
    public int await() throws InterruptedException {
        int res0 = res;

        if (res0 == 0) {
            synchronized (this) {
                while ((res0 = res) == 0)
                    wait();
            }
        }

        assert res0 != 0;

        return res0;
    }

    /**
     * Notifies all waiting threads to finish waiting.
     *
     * @param res Operation result to return from {@link #await()}.
     */
    public void finish(int res) {
        assert res != 0;

        synchronized (this) {
            this.res = res;

            notifyAll();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StreamerIndexUpdateSync.class, this);
    }
}
