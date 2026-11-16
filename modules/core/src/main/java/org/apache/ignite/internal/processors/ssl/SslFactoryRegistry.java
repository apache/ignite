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
import java.util.concurrent.ScheduledExecutorService;
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
import org.apache.ignite.internal.thread.context.function.OperationContextAwareRunnable;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslContextFactory;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;

/** SSL factories used by running transports. */
public class SslFactoryRegistry {
    /** */
    private static final long CHECK_PERIOD = HOURS.toMillis(1);

    /** */
    private final Map<String, Factory<SSLContext>> factories = new LinkedHashMap<>();

    /** */
    private volatile Map<Factory<SSLContext>, HotReload> hotReloads = Collections.emptyMap();

    /** */
    private volatile ScheduledExecutorService executor;

    /** */
    private volatile IgniteLogger log;

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

        log = ctx.log(getClass());

        Map<Factory<SSLContext>, String> transports = new IdentityHashMap<>();

        factories().forEach((transport, factory) -> transports.merge(factory, transport, (names, name) -> names + ", " + name));

        Map<Factory<SSLContext>, HotReload> hotReloads = new IdentityHashMap<>();

        for (Map.Entry<Factory<SSLContext>, String> entry : transports.entrySet()) {
            if (entry.getKey() instanceof SslContextFactory sslFactory && sslFactory.isHotReloadEnabled()) {
                if (sslFactory.getHotReloadTime() == 0)
                    sslFactory.init();

                HotReload hotReload = new HotReload(sslFactory, entry.getValue());

                hotReload.logHotReloadTime();
                hotReloads.put(sslFactory, hotReload);
            }
        }

        if (hotReloads.isEmpty())
            return;

        executor = newSingleThreadScheduledExecutor("ssl-hot-reload", ctx.igniteInstanceName());

        executor.scheduleWithFixedDelay(new OperationContextAwareRunnable(() -> hotReloads.values().forEach(HotReload::check), null),
            0, CHECK_PERIOD, MILLISECONDS);

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
    }

    /**
     * Reloads the factory and resets its hot reload failure count.
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

    /** Hot reload state of one factory. */
    private class HotReload {
        /** */
        private final SslContextFactory factory;

        /** */
        private final String transports;

        /** */
        private final AtomicInteger failures = new AtomicInteger();

        /** */
        private HotReload(SslContextFactory factory, String transports) {
            this.factory = factory;
            this.transports = transports;
        }

        /** Reloads the factory if its hot reload time has come. */
        private void check() {
            long hotReloadTime = factory.getHotReloadTime();

            if (hotReloadTime == 0 || hotReloadTime > System.currentTimeMillis())
                return;

            automaticHotReloadAttempts.incrementAndGet();

            if (log.isInfoEnabled())
                log.info("TLS hot reload started [transports=" + transports + ']');

            try {
                factory.init();
            }
            catch (Exception e) {
                fail(e);

                return;
            }

            hotReloadTime = factory.getHotReloadTime();

            if (hotReloadTime != 0 && hotReloadTime <= System.currentTimeMillis()) {
                fail(new IgniteException("Loaded certificates are still due for hot reload [hotReloadTime="
                    + new Date(hotReloadTime) + ']'));
            }
            else {
                failures.set(0);

                if (log.isInfoEnabled())
                    log.info("TLS hot reload succeeded [transports=" + transports + ']');

                logHotReloadTime();
            }
        }

        /** */
        private void fail(Exception cause) {
            if (executor.isShutdown())
                return;

            failures.incrementAndGet();

            log.warning("TLS hot reload failed [transports=" + transports + ", nextAttempt="
                + new Date(System.currentTimeMillis() + CHECK_PERIOD) + ']', cause);
        }

        /** */
        private void reload() {
            reloadFactory(factory);

            failures.set(0);

            logHotReloadTime();
        }

        /** */
        private void logHotReloadTime() {
            long hotReloadTime = factory.getHotReloadTime();

            if (hotReloadTime == 0) {
                log.warning("TLS hot reload will not run: no private key with an X.509 certificate chain was found [transports="
                    + transports + ']');
            }
            else if (log.isInfoEnabled())
                log.info("TLS hot reload due [transports=" + transports + ", hotReloadTime=" + new Date(hotReloadTime) + ']');
        }
    }
}
