/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;

import java.util.concurrent.atomic.*;

/**
 * Abstract class for GGFS managers.
 */
public abstract class GridGgfsManager {
    /** GGFS context. */
    protected GridGgfsContext ggfsCtx;

    /** Logger. */
    protected GridLogger log;

    /** Starting flag. */
    private AtomicBoolean starting = new AtomicBoolean();

    /**
     * Called when GGFS processor is started.
     *
     * @param ggfsCtx GGFS context.
     */
    public void start(GridGgfsContext ggfsCtx) throws GridException {
        if (!starting.compareAndSet(false, true))
            assert false : "Method start is called more than once for manager: " + this;

        assert ggfsCtx != null;

        this.ggfsCtx = ggfsCtx;

        log = ggfsCtx.kernalContext().log(getClass());

        start0();

        if (log != null && log.isDebugEnabled())
            log.debug(startInfo());
    }

    /**
     * Stops manager.
     *
     * @param cancel Cancel flag.
     */
    public final void stop(boolean cancel) {
        if (!starting.get())
            // Ignoring attempt to stop manager that has never been started.
            return;

        stop0(cancel);

        if (log != null && log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * @throws GridException If failed.
     */
    public final void onKernalStart() throws GridException {
        onKernalStart0();

        if (log != null && log.isDebugEnabled())
            log.debug(kernalStartInfo());
    }

    /**
     * @param cancel Cancel flag.
     */
    public final void onKernalStop(boolean cancel) {
        if (!starting.get())
            // Ignoring attempt to stop manager that has never been started.
            return;

        onKernalStop0(cancel);

        if (log != null && log.isDebugEnabled())
            log.debug(kernalStopInfo());
    }

    /**
     * Start manager implementation.
     */
    protected void start0() throws GridException {
        // No-op by default.
    }

    /**
     * Stop manager implementation.
     *
     * @param cancel Cancel flag.
     */
    protected void stop0(boolean cancel) {
        // No-op by default.
    }

    /**
     * @throws GridException If failed.
     */
    protected void onKernalStart0() throws GridException {
        // No-op.
    }

    /**
     *
     */
    protected void onKernalStop0(boolean cancel) {
        // No-op.
    }

    /**
     * @return Start info.
     */
    protected String startInfo() {
        return "Cache manager started: " + getClass().getSimpleName();
    }

    /**
     * @return Stop info.
     */
    protected String stopInfo() {
        return "Cache manager stopped: " + getClass().getSimpleName();
    }

    /**
     * @return Start info.
     */
    protected String kernalStartInfo() {
        return "Cache manager received onKernalStart() callback: " + getClass().getSimpleName();
    }

    /**
     * @return Stop info.
     */
    protected String kernalStopInfo() {
        return "Cache manager received onKernalStop() callback: " + getClass().getSimpleName();
    }
}
