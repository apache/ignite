/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl;

import org.jetbrains.annotations.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Delegating thread factory which forces all spawned thread to be daemons.
 */
public class GridClientThreadFactory implements ThreadFactory {
    /** Pool number. */
    private static final AtomicInteger poolCtr = new AtomicInteger(1);

    /** Thread number. */
    private final AtomicInteger threadCtr = new AtomicInteger(1);

    /** Prefix. */
    private final String prefix;

    /** Daemon flag. */
    private final boolean daemon;

    /**
     * Constructor.
     *
     * @param name Name prefix.
     * @param daemon Daemon flag.
     */
    public GridClientThreadFactory(String name, boolean daemon) {
        this.daemon = daemon;

        prefix = "gridgain-client-" + name + "-" + poolCtr.getAndIncrement() + "-";
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(@NotNull Runnable r) {
        Thread thread = new Thread(r, prefix + threadCtr.incrementAndGet());

        if (daemon)
            thread.setDaemon(true);

        return thread;
    }
}
