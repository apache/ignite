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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslContextFactory;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;

/** SSL factories used by running transports. */
public class SslFactoryRegistry {
    /** */
    private static final long CHECK_PERIOD = HOURS.toMillis(1);

    /** */
    private final List<SslFactoryWrapper> factories = new CopyOnWriteArrayList<>();

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
        for (int i = 0; i < factories.size(); i++) {
            SslFactoryWrapper wrapper = factories.get(i);

            if (wrapper.factory == factory) {
                factories.set(i, new SslFactoryWrapper(factory, wrapper.transports + ", " + transport));

                return;
            }
        }

        factories.add(new SslFactoryWrapper(factory, transport));
    }

    /** @return Registered factories in registration order, each with its transports joined by commas. */
    public List<Entry<Factory<SSLContext>, String>> factories() {
        return factories.stream().map(wrapper -> Map.entry(wrapper.factory, wrapper.transports)).toList();
    }

    /** */
    public synchronized void start(GridKernalContext ctx) {
        MetricRegistryImpl metrics = ctx.metric().registry("ssl");

        metrics.register("ConsecutiveHotReloadFailures", () -> factories.stream().mapToLong(w -> w.failures.get()).sum(),
            "Sum of consecutive automatic certificate hot reload failures across SSL factories.");
        metrics.register("ManualReloadAttempts", manualReloadAttempts::get, "Number of manual SSL factory reload attempts.");
        metrics.register("AutomaticHotReloadAttempts", automaticHotReloadAttempts::get,
            "Number of automatic SSL factory hot reload attempts.");

        log = ctx.log(getClass());

        boolean hotReload = false;

        for (SslFactoryWrapper wrapper : factories) {
            if (wrapper.factory instanceof SslContextFactory sslFactory && sslFactory.isHotReloadEnabled()) {
                wrapper.hotReloadFactory = sslFactory;
                wrapper.logHotReloadTime(sslFactory.getHotReloadTime());

                hotReload = true;
            }
        }

        if (!hotReload)
            return;

        executor = newSingleThreadScheduledExecutor("ssl-hot-reload", ctx.igniteInstanceName());

        executor.scheduleWithFixedDelay(() -> factories.forEach(SslFactoryWrapper::check), 0, CHECK_PERIOD, MILLISECONDS);
    }

    /** */
    public synchronized void stop() {
        if (executor != null)
            executor.shutdownNow();
    }

    /**
     * Reloads a registered factory. For a factory with hot reload enabled, waits for its running check and resets its failure
     * count.
     *
     * @param factory Registered factory to reload.
     */
    public void reload(AbstractSslContextFactory factory) {
        for (SslFactoryWrapper wrapper : factories) {
            if (wrapper.factory == factory)
                wrapper.reload();
        }
    }

    /** Registered factory with its transports and hot reload state. Its check and manual reload hold its monitor. */
    private class SslFactoryWrapper {
        /** */
        private final Factory<SSLContext> factory;

        /** */
        private final String transports;

        /** The factory if hot reload is enabled for it and the registry has started. */
        @Nullable private volatile SslContextFactory hotReloadFactory;

        /** */
        private final AtomicInteger failures = new AtomicInteger();

        /** */
        private SslFactoryWrapper(Factory<SSLContext> factory, String transports) {
            this.factory = factory;
            this.transports = transports;
        }

        /** Reloads the factory if its hot reload time has come, and ends the failure streak if it has not. */
        private synchronized void check() {
            SslContextFactory sslFactory = hotReloadFactory;

            if (sslFactory == null || executor.isShutdown())
                return;

            if (!due(sslFactory.getHotReloadTime())) {
                failures.set(0);

                return;
            }

            automaticHotReloadAttempts.incrementAndGet();

            if (log.isInfoEnabled())
                log.info("TLS hot reload started [transports=" + transports + ']');

            try {
                sslFactory.init();
            }
            catch (Exception e) {
                fail(e);

                return;
            }

            long hotReloadTime = sslFactory.getHotReloadTime();

            if (due(hotReloadTime)) {
                fail(new IgniteException("Loaded certificates are still due for hot reload [hotReloadTime="
                    + new Date(hotReloadTime) + ']'));
            }
            else {
                failures.set(0);

                if (log.isInfoEnabled())
                    log.info("TLS hot reload succeeded [transports=" + transports + ']');

                logHotReloadTime(hotReloadTime);
            }
        }

        /** @return Whether the loaded certificates have a hot reload time, and it has come. */
        private static boolean due(long hotReloadTime) {
            return hotReloadTime != 0 && hotReloadTime <= System.currentTimeMillis();
        }

        /** */
        private void fail(Exception cause) {
            if (executor.isShutdown())
                return;

            failures.incrementAndGet();

            log.warning("TLS hot reload failed [transports=" + transports + ", nextAttemptNotBefore="
                + new Date(System.currentTimeMillis() + CHECK_PERIOD) + ']', cause);
        }

        /** */
        private synchronized void reload() {
            manualReloadAttempts.incrementAndGet();

            ((AbstractSslContextFactory)factory).init();

            SslContextFactory sslFactory = hotReloadFactory;

            if (sslFactory != null) {
                failures.set(0);

                logHotReloadTime(sslFactory.getHotReloadTime());
            }
        }

        /** */
        private void logHotReloadTime(long hotReloadTime) {
            if (hotReloadTime == 0) {
                log.warning("TLS hot reload will not run: no private key with an X.509 certificate chain was found [transports="
                    + transports + ']');
            }
            else if (log.isInfoEnabled())
                log.info("TLS hot reload due [transports=" + transports + ", hotReloadTime=" + new Date(hotReloadTime) + ']');
        }
    }
}
