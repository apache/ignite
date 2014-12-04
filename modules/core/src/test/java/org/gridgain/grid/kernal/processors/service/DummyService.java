/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.apache.ignite.managed.*;
import org.jdk8.backport.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Dummy service.
 */
public class DummyService implements GridService {
    /** */
    private static final long serialVersionUID = 0L;

    /** Inited counter per service. */
    public static final ConcurrentMap<String, AtomicInteger> started = new ConcurrentHashMap8<>();

    /** Started counter per service. */
    public static final ConcurrentMap<String, AtomicInteger> inited = new ConcurrentHashMap8<>();

    /** Latches per service. */
    private static final ConcurrentMap<String, CountDownLatch> exeLatches = new ConcurrentHashMap8<>();

    /** Cancel latches per service. */
    private static final ConcurrentMap<String, CountDownLatch> cancelLatches = new ConcurrentHashMap8<>();

    /** Cancelled flags per service. */
    private static final ConcurrentMap<String, AtomicInteger> cancelled = new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override public void cancel(GridServiceContext ctx) {
        AtomicInteger cntr = cancelled.get(ctx.name());

        if (cntr == null) {
            AtomicInteger old = cancelled.putIfAbsent(ctx.name(), cntr = new AtomicInteger());

            if (old != null)
                cntr = old;
        }

        cntr.incrementAndGet();

        CountDownLatch latch = cancelLatches.get(ctx.name());

        if (latch != null)
            latch.countDown();

        System.out.println("Cancelling service: " + ctx.name());
    }

    /** {@inheritDoc} */
    @Override public void init(GridServiceContext ctx) throws Exception {
        AtomicInteger cntr = inited.get(ctx.name());

        if (cntr == null) {
            AtomicInteger old = inited.putIfAbsent(ctx.name(), cntr = new AtomicInteger());

            if (old != null)
                cntr = old;
        }

        cntr.incrementAndGet();

        System.out.println("Initializing service: " + ctx.name());
    }

    /** {@inheritDoc} */
    @Override public void execute(GridServiceContext ctx) {
        AtomicInteger cntr = started.get(ctx.name());

        if (cntr == null) {
            AtomicInteger old = started.putIfAbsent(ctx.name(), cntr = new AtomicInteger());

            if (old != null)
                cntr = old;
        }

        cntr.incrementAndGet();

        System.out.println("Executing service: " + ctx.name());

        CountDownLatch latch = exeLatches.get(ctx.name());

        if (latch != null)
            latch.countDown();
    }

    /**
     * @return Cancelled flag.
     */
    public static int cancelled(String name) {
        AtomicInteger cntr = cancelled.get(name);

        return cntr == null ? 0 : cntr.get();
    }

    /**
     * @return Started counter.
     */
    public static int started(String name) {
        AtomicInteger cntr = started.get(name);

        return cntr == null ? 0 : cntr.get();
    }

    /**
     * Resets dummy service to initial state.
     */
    public static void reset() {
        System.out.println("Resetting dummy service.");

        started.clear();
        exeLatches.clear();
        cancelled.clear();
    }

    /**
     * @param latch Count down latch.
     */
    public static void exeLatch(String name, CountDownLatch latch) {
        exeLatches.put(name, latch);
    }

    /**
     * @param latch Cancel latch.
     */
    public static void cancelLatch(String name, CountDownLatch latch) {
        cancelLatches.put(name, latch);
    }
}
