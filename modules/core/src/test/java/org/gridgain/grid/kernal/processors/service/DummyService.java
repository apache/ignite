// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.service.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Dummy service.
 *
 * @author @java.author
 * @version @java.version
 */
public class DummyService implements GridService {
    /** Started counter. */
    public static final AtomicInteger started = new AtomicInteger();

    private static CountDownLatch latch;

    /** Cancelled flag. */
    public static volatile boolean cancelled = false;

    /** {@inheritDoc} */
    @Override public void cancel(GridServiceContext ctx) {
        cancelled = true;

        System.out.println("Cancelling service: " + ctx.name());
    }

    /** {@inheritDoc} */
    @Override public void execute(GridServiceContext ctx) {
        started.incrementAndGet();

        System.out.println("Executing service: " + ctx.name());

        if (latch != null)
            latch.countDown();
    }

    /**
     * @return Cancelled flag.
     */
    public static boolean isCancelled() {
        return cancelled;
    }

    /**
     * @return Started counter.
     */
    public static int started() {
        return started.get();
    }

    /**
     * Resets dummy service to initial state.
     */
    public static void reset() {
        started.set(0);
        latch = null;
        cancelled = false;
    }

    /**
     * @param latch Count down latch.
     */
    public static void latch(CountDownLatch latch) {
        DummyService.latch = latch;
    }
}
