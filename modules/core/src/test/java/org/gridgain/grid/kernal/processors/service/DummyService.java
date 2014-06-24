// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.service.*;

/**
 * Dummy service.
 *
 * @author @java.author
 * @version @java.version
 */
public class DummyService implements GridService {
    /** Started flag. */
    public static volatile boolean started = false;

    /** Cancelled flag. */
    public static volatile boolean cancelled = false;

    /** {@inheritDoc} */
    @Override public void cancel(GridServiceContext ctx) {
        cancelled = true;

        System.out.println("Cancelling service: " + ctx.name());
    }

    /** {@inheritDoc} */
    @Override public void execute(GridServiceContext ctx) {
        started = true;

        System.out.println("Executing service: " + ctx.name());
    }

    /**
     * @return Cancelled flag.
     */
    public static boolean isCancelled() {
        return cancelled;
    }

    /**
     * @return Started flag.
     */
    public static boolean isStarted() {
        return started;
    }
}
