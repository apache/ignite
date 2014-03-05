/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.util.typedef.*;

/**
 * GGFS ad-hoc thread.
 */
public abstract class GridGgfsThread extends Thread {
    /**
     * Creates {@code GGFS} add-hoc thread.
     */
    protected GridGgfsThread() {
        super("ggfs-worker");
    }

    /**
     * Creates {@code GGFS} add-hoc thread.
     *
     * @param name Thread name.
     */
    protected GridGgfsThread(String name) {
        super(name);
    }

    /** {@inheritDoc} */
    @Override public final void run() {
        try {
            body();
        }
        catch (InterruptedException ignore) {
            interrupt();
        }
        // Catch all.
        catch (Throwable e) {
            X.error("Failed to execute GGFS ad-hoc thread: " + e.getMessage());

            e.printStackTrace();
        }
        finally {
            try {
                cleanup();
            }
            // Catch all.
            catch (Throwable e) {
                X.error("Failed to clean up GGFS ad-hoc thread: " + e.getMessage());

                e.printStackTrace();
            }
        }
    }

    /**
     * Thread body.
     *
     * @throws InterruptedException If interrupted.
     */
    protected abstract void body() throws InterruptedException;

    /**
     * Cleanup.
     */
    protected void cleanup() {
        // No-op.
    }
}
