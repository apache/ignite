/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.vault.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of vault {@link Watcher}.
 */
public class WatcherImpl implements Watcher {
    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(WatcherImpl.class);

    /** Queue for changed vault entries. */
    private final BlockingQueue<Entry> queue = new LinkedBlockingQueue<>();

    /** Registered vault watches. */
    private final Map<Long, VaultWatch> watches = new HashMap<>();

    /** Flag for indicating if watcher is stopped. */
    private volatile boolean stop;

    /** Counter for generating ids for watches. */
    private AtomicLong watchIds;

    /** Mutex. */
    private final Object mux = new Object();

    /** Execution service which runs thread for processing changed vault entries. */
    private final ExecutorService exec;

    /**
     * Default constructor.
     */
    public WatcherImpl() {
        watchIds = new AtomicLong(0);

        exec = Executors.newFixedThreadPool(1);

        exec.execute(new WatcherWorker());
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Long> register(@NotNull VaultWatch vaultWatch) {
        synchronized (mux) {
            Long key = watchIds.incrementAndGet();

            watches.put(key, vaultWatch);

            return CompletableFuture.completedFuture(key);
        }
    }

    /** {@inheritDoc} */
    @Override public void notify(@NotNull Entry val) {
        queue.offer(val);
    }

    /** {@inheritDoc} */
    @Override public void cancel(@NotNull Long id) {
        synchronized (mux) {
            watches.remove(id);
        }
    }

    /**
     * Shutdowns watcher.
     */
    public void shutdown() {
        stop = true;

        if (exec != null) {
            List<Runnable> tasks = exec.shutdownNow();

            if (!tasks.isEmpty())
                LOG.warn("Runnable tasks outlived thread pool executor service");

            try {
                exec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                LOG.warn("Got interrupted while waiting for executor service to stop.");

                exec.shutdownNow();

                // Preserve interrupt status.
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Worker that polls changed vault entries from queue and notifies registered watches.
     */
    private class WatcherWorker implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            while (!stop) {
                try {
                    Entry val = queue.take();

                    synchronized (mux) {
                        watches.forEach((k, w) -> {
                            if (!w.notify(val))
                                cancel(k);
                        });
                    }
                }
                catch (InterruptedException interruptedException) {
                    synchronized (mux) {
                        watches.forEach((k, w) -> {
                            w.onError(
                                new IgniteInternalCheckedException(
                                    "Error occurred during watches handling ",
                                    interruptedException.getCause()));

                            cancel(k);
                        });
                    }
                }
            }
        }
    }
}
