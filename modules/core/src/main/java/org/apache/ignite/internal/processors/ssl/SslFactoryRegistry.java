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

package org.apache.ignite.internal.processors.ssl;

import java.util.Collections;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslContextFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;

/** SSL factories used by running transports. */
public class SslFactoryRegistry {
    /** */
    private final Map<String, Factory<SSLContext>> factories = new LinkedHashMap<>();

    /** */
    private volatile Map<Factory<SSLContext>, HotReload> hotReloads = Collections.emptyMap();

    /** */
    private volatile ScheduledExecutorService executor;

    /** */
    private final AtomicLong manualReloadAttempts = new AtomicLong();

    /** */
    private final AtomicLong automaticHotReloadAttempts = new AtomicLong();

    /** */
    public synchronized void register(String transport, Factory<SSLContext> factory) {
        if (factories.putIfAbsent(transport, factory) != null)
            throw new IllegalStateException("SSL factory is already registered [transport=" + transport + ']');
    }

    /** @return Snapshot of active transport factories. */
    public synchronized Map<String, Factory<SSLContext>> factories() {
        return new LinkedHashMap<>(factories);
    }

    /** */
    public synchronized void start(GridKernalContext ctx) {
        MetricRegistryImpl metrics = ctx.metric().registry("ssl");

        metrics.register("ConsecutiveHotReloadFailures", () -> hotReloads.values().stream().mapToLong(r -> r.failures.get()).sum(),
            "Sum of consecutive automatic certificate hot reload failures across SSL factories.");
        metrics.register("ManualReloadAttempts", manualReloadAttempts::get, "Number of manual SSL factory reload attempts.");
        metrics.register("AutomaticHotReloadAttempts", automaticHotReloadAttempts::get,
            "Number of automatic SSL factory hot reload attempts.");

        Map<String, Factory<SSLContext>> factories = factories();
        Map<Factory<SSLContext>, HotReload> hotReloads = new IdentityHashMap<>();
        IgniteLogger log = ctx.log(getClass());

        for (Factory<SSLContext> factory : factories.values()) {
            if (factory instanceof SslContextFactory sslFactory && sslFactory.isHotReloadEnabled() && !hotReloads.containsKey(factory)) {
                if (sslFactory.getHotReloadTime() == 0)
                    sslFactory.init();

                String transports = factories.entrySet().stream()
                    .filter(entry -> entry.getValue() == factory)
                    .map(Map.Entry::getKey)
                    .collect(joining(", "));

                hotReloads.put(factory, new HotReload(sslFactory, transports, log));
            }
        }

        if (hotReloads.isEmpty() || ctx.isStopping())
            return;

        executor = newSingleThreadScheduledExecutor("ssl-hot-reload", ctx.igniteInstanceName());

        try (Scope ignored = OperationContext.restoreSnapshot(null)) {
            executor.execute(() -> hotReloads.values().forEach(HotReload::scheduleHotReload));
        }

        this.hotReloads = hotReloads;
    }

    /** */
    public synchronized void stop() {
        ScheduledExecutorService executor = this.executor;

        if (executor == null)
            return;

        for (Runnable task : executor.shutdownNow()) {
            if (task instanceof Future<?> fut)
                fut.cancel(false);
        }

        boolean interrupted = false;

        while (!executor.isTerminated()) {
            try {
                executor.awaitTermination(Long.MAX_VALUE, MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Reloads the factory and resets its automatic hot reload schedule and failure count.
     *
     * @param factory Factory to reload.
     * @throws IgniteCheckedException If hot reload has not started, or an asynchronous reload fails or is interrupted.
     */
    public void reload(AbstractSslContextFactory factory) throws IgniteCheckedException {
        Future<?> reload;

        synchronized (this) {
            HotReload hotReload = hotReloads.get(factory);

            if (hotReload == null && factory instanceof SslContextFactory sslFactory && sslFactory.isHotReloadEnabled())
                throw new IgniteCheckedException("Automatic TLS hot reload has not started.");

            reload = hotReload == null ? null : executor.submit(hotReload::reload);
        }

        if (reload == null)
            reloadFactory(factory);
        else {
            try {
                U.get(reload);
            }
            catch (IgniteInterruptedCheckedException e) {
                reload.cancel(true);

                throw e;
            }
        }
    }

    /** */
    private void reloadFactory(AbstractSslContextFactory factory) {
        manualReloadAttempts.incrementAndGet();

        factory.init();
    }

    /** */
    private class HotReload implements Runnable {
        /** */
        private final SslContextFactory factory;

        /** */
        private final String transports;

        /** */
        private final IgniteLogger log;

        /** */
        private ScheduledFuture<?> pending;

        /** */
        private final AtomicInteger failures = new AtomicInteger();

        /** */
        private HotReload(SslContextFactory factory, String transports, IgniteLogger log) {
            this.factory = factory;
            this.transports = transports;
            this.log = log;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            if (executor.isShutdown())
                return;

            automaticHotReloadAttempts.incrementAndGet();

            try {
                factory.hotReload();
            }
            catch (Exception e) {
                retry(e);

                return;
            }

            long hotReloadTime = factory.getHotReloadTime();

            if (hotReloadTime != 0 && hotReloadTime <= System.currentTimeMillis())
                retry(new IgniteException("Loaded certificates are still due for hot reload [hotReloadTime="
                    + new Date(hotReloadTime) + ']'));
            else
                scheduleHotReload();
        }

        /** */
        private void retry(Exception cause) {
            if (executor.isShutdown())
                return;

            long delay = Math.min(MINUTES.toMillis(1) << Math.min(failures.incrementAndGet() - 1, 6), MINUTES.toMillis(60));
            long nextAttempt = System.currentTimeMillis() + delay + ThreadLocalRandom.current().nextLong(delay / 2 + 1);

            log.warning("TLS hot reload failed [transports=" + transports + ", nextAttempt=" + new Date(nextAttempt) + ']', cause);

            schedule(nextAttempt);
        }

        /** */
        private void reload() {
            reloadFactory(factory);
            scheduleHotReload();
        }

        /** Ends the failure streak and schedules the next attempt at the hot reload time. */
        private void scheduleHotReload() {
            failures.set(0);
            schedule(factory.getHotReloadTime());
        }

        /** */
        private void schedule(long time) {
            if (pending != null)
                pending.cancel(false);

            if (time == 0)
                return;

            try (Scope ignored = OperationContext.restoreSnapshot(null)) {
                pending = executor.schedule(this, Math.max(0, time - System.currentTimeMillis()), MILLISECONDS);
            }
            catch (RejectedExecutionException e) {
                if (!executor.isShutdown())
                    throw e;

                return;
            }

            if (log.isInfoEnabled())
                log.info("TLS hot reload scheduled [transports=" + transports + ", nextAttempt=" + new Date(time) + ']');
        }
    }
}
