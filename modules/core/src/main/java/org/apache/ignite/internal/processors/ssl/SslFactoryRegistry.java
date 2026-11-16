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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareRunnable;
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

    /** @return Registered factories in registration order, each under its transports joined by commas. */
    public synchronized Map<String, Factory<SSLContext>> factories() {
        Map<Factory<SSLContext>, String> names = new IdentityHashMap<>();

        factories.forEach((transport, factory) -> names.merge(factory, transport, (prev, next) -> prev + ", " + next));

        Map<String, Factory<SSLContext>> transports = new LinkedHashMap<>();

        factories.values().forEach(factory -> transports.putIfAbsent(names.get(factory), factory));

        return transports;
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

        Map<Factory<SSLContext>, HotReload> hotReloads = new IdentityHashMap<>();

        for (Map.Entry<String, Factory<SSLContext>> entry : factories().entrySet()) {
            if (entry.getValue() instanceof SslContextFactory sslFactory && sslFactory.isHotReloadEnabled()) {
                if (sslFactory.getHotReloadTime() == 0)
                    sslFactory.init();

                HotReload hotReload = new HotReload(sslFactory, entry.getKey());

                hotReload.logHotReloadTime();
                hotReloads.put(sslFactory, hotReload);
            }
        }

        if (hotReloads.isEmpty())
            return;

        this.hotReloads = hotReloads;

        executor = newSingleThreadScheduledExecutor("ssl-hot-reload", ctx.igniteInstanceName());

        executor.scheduleWithFixedDelay(new OperationContextAwareRunnable(() -> hotReloads.values().forEach(HotReload::check), null),
            0, CHECK_PERIOD, MILLISECONDS);
    }

    /** */
    public synchronized void stop() {
        if (executor != null)
            executor.shutdownNow();
    }

    /**
     * Reloads the factory. For a factory with hot reload enabled, waits for its running check and resets its failure count.
     *
     * @param factory Factory to reload.
     */
    public void reload(AbstractSslContextFactory factory) {
        HotReload hotReload = hotReloads.get(factory);

        if (hotReload == null)
            reloadFactory(factory);
        else
            hotReload.reload();
    }

    /** */
    private void reloadFactory(AbstractSslContextFactory factory) {
        manualReloadAttempts.incrementAndGet();

        factory.init();
    }

    /** Hot reload state of one factory. Its check and manual reload hold its monitor. */
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
        private synchronized void check() {
            if (!due())
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

            if (due()) {
                fail(new IgniteException("Loaded certificates are still due for hot reload [hotReloadTime="
                    + new Date(factory.getHotReloadTime()) + ']'));
            }
            else {
                failures.set(0);

                if (log.isInfoEnabled())
                    log.info("TLS hot reload succeeded [transports=" + transports + ']');

                logHotReloadTime();
            }
        }

        /** @return Whether the loaded certificates have a hot reload time, and it has come. */
        private boolean due() {
            long hotReloadTime = factory.getHotReloadTime();

            return hotReloadTime != 0 && hotReloadTime <= System.currentTimeMillis();
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
        private synchronized void reload() {
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
