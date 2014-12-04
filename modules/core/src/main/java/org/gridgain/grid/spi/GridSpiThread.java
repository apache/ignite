/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.util.concurrent.atomic.*;

/**
 * This class provides convenient adapter for threads used by SPIs.
 * This class adds necessary plumbing on top of the {@link Thread} class:
 * <ul>
 * <li>Consistent naming of threads</li>
 * <li>Dedicated parent thread group</li>
 * </ul>
 */
public abstract class GridSpiThread extends Thread {
    /** Default thread's group. */
    public static final ThreadGroup DFLT_GRP = new ThreadGroup("gridgain-spi");

    /** Number of all system threads in the system. */
    private static final AtomicLong cntr = new AtomicLong();

    /** Grid logger. */
    private final IgniteLogger log;

    /**
     * Creates thread with given {@code name}.
     *
     * @param gridName Name of grid this thread is created in.
     * @param name Thread's name.
     * @param log Grid logger to use.
     */
    protected GridSpiThread(String gridName, String name, IgniteLogger log) {
        super(DFLT_GRP, name + "-#" + cntr.incrementAndGet() + '%' + gridName);

        assert log != null;

        this.log = log;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CatchGenericClass"})
    @Override public final void run() {
        try {
            body();
        }
        catch (InterruptedException e) {
            if (log.isDebugEnabled())
                log.debug("Caught interrupted exception: " + e);

            Thread.currentThread().interrupt();
        }
        // Catch everything to make sure that it gets logged properly and
        // not to kill any threads from the underlying thread pool.
        catch (Throwable e) {
            U.error(log, "Runtime error caught during grid runnable execution: " + this, e);
        }
        finally {
            cleanup();

            if (log.isDebugEnabled()) {
                if (isInterrupted())
                    log.debug("Grid runnable finished due to interruption without cancellation: " + getName());
                else
                    log.debug("Grid runnable finished normally: " + getName());
            }
        }
    }

    /**
     * Should be overridden by child classes if cleanup logic is required.
     */
    protected void cleanup() {
        // No-op.
    }

    /**
     * Body of SPI thread.
     *
     * @throws InterruptedException If thread got interrupted.
     */
    protected abstract void body() throws InterruptedException;

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSpiThread.class, this, "name", getName());
    }
}
