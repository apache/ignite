/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.impl;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;

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

        prefix = "ignite-client-" + name + "-" + poolCtr.getAndIncrement() + "-";
    }

    /** {@inheritDoc} */
    @Override public Thread newThread(@NotNull Runnable r) {
        Thread thread = new Thread(r, prefix + threadCtr.incrementAndGet());

        if (daemon)
            thread.setDaemon(true);

        return thread;
    }
}