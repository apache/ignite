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

import java.util.ArrayList;
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
import org.apache.ignite.internal.thread.context.function.OperationContextAwareRunnable;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;

/** SSL factories used by running transports. */
public class SslFactoryRegistry {
    /** */
    private static final long CHECK_PERIOD = HOURS.toMillis(1);

    /** */
    private final List<RegisteredFactory> factories = new CopyOnWriteArrayList<>();

    /** */
    private volatile ScheduledExecutorService executor;

    /** Logs nothing until the registry starts. */
    private volatile IgniteLogger log = new NullLogger();

    /** */
    private final AtomicLong manualReloadAttempts = new AtomicLong();

    /** */
    private final AtomicLong automaticHotReloadAttempts = new AtomicLong();

    /** */
    public synchronized void register(String transport, Factory<SSLContext> factory) {
        for (int i = 0; i < factories.size(); i++) {
            RegisteredFactory registered = factories.get(i);

            if (registered.factory == factory) {
                factories.set(i, new RegisteredFactory(factory, registered.transports + ", " + transport));

                return;
            }
        }

        factories.add(new RegisteredFactory(factory, transport));
    }

    /** @return Registered factories in registration order, each with its transports joined by commas. */
    public List<Entry<Factory<SSLContext>, String>> factories() {
        List<Entry<Factory<SSLContext>, String>> res = new ArrayList<>(factories.size());

        for (RegisteredFactory registered : factories)
            res.add(Map.entry(registered.factory, registered.transports));

        return res;
    }

    /** */
    public void start(GridKernalContext ctx) {
        MetricRegistryImpl metrics = ctx.metric().registry("ssl");

        metrics.register("ConsecutiveHotReloadFailures", this::consecutiveHotReloadFailures,
            "Number of consecutive automatic hot reload failures.");
        metrics.register("ManualReloadAttempts", manualReloadAttempts::get, "Number of manual reload attempts.");
        metrics.register("AutomaticHotReloadAttempts", automaticHotReloadAttempts::get,
            "Number of automatic hot reload attempts.");

        log = ctx.log(getClass());

        boolean hotReload = false;

        for (RegisteredFactory registered : factories) {
            AbstractSslContextFactory sslFactory = registered.hotReloadFactory();

            if (sslFactory != null) {
                registered.logHotReloadTime(sslFactory.getHotReloadTime());

                hotReload = true;
            }
        }

        if (!hotReload)
            return;

        executor = newSingleThreadScheduledExecutor("ssl-hot-reload", ctx.igniteInstanceName());

        executor.scheduleWithFixedDelay(new OperationContextAwareRunnable(this::checkFactories, null),
            0, CHECK_PERIOD, MILLISECONDS);
    }

    /** @return Sum of consecutive automatic hot reload failures across factories. */
    private long consecutiveHotReloadFailures() {
        long sum = 0;

        for (RegisteredFactory registered : factories)
            sum += registered.failures.get();

        return sum;
    }

    /** Runs the hourly check of the factories with hot reload enabled. */
    private void checkFactories() {
        if (log.isDebugEnabled())
            log.debug("TLS hot reload check started");

        for (RegisteredFactory registered : factories) {
            if (registered.hotReloadFactory() != null)
                registered.check();
        }
    }

    /** */
    public void stop() {
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
        for (RegisteredFactory registered : factories) {
            if (registered.factory == factory)
                registered.reload();
        }
    }

    /** Registered factory with its transports and hot reload state. The hourly check and manual reload hold its monitor. */
    private class RegisteredFactory {
        /** */
        private final Factory<SSLContext> factory;

        /** */
        private final String transports;

        /** */
        private final AtomicInteger failures = new AtomicInteger();

        /** */
        private RegisteredFactory(Factory<SSLContext> factory, String transports) {
            this.factory = factory;
            this.transports = transports;
        }

        /** @return The factory if hot reload is enabled for it, {@code null} otherwise. */
        @Nullable private AbstractSslContextFactory hotReloadFactory() {
            return factory instanceof AbstractSslContextFactory sslFactory && sslFactory.isHotReloadEnabled() ? sslFactory : null;
        }

        /**
         * Checks the factory unless the registry has stopped. Counts and logs a failed check, so it does not stop
         * the other factories or the schedule. A check that fails while the registry stops is logged at debug level
         * and not counted.
         */
        private synchronized void check() {
            if (executor.isShutdown())
                return;

            try {
                reloadIfDue();
            }
            catch (Throwable e) {
                if (!executor.isShutdown()) {
                    failures.incrementAndGet();

                    log.warning("TLS hot reload failed [transports=" + transports + ", nextAttempt="
                        + new Date(System.currentTimeMillis() + CHECK_PERIOD) + ']', e);
                }
                else if (log.isDebugEnabled())
                    log.debug("TLS hot reload failed during node stop [transports=" + transports + ", err=" + e + ']');
            }
        }

        /**
         * Reloads the factory if its hot reload time has come, and ends the failure streak if it has not.
         *
         * @throws IgniteException If the reload fails or leaves certificates that are still due.
         */
        private void reloadIfDue() {
            AbstractSslContextFactory sslFactory = (AbstractSslContextFactory)factory;

            if (!due(sslFactory.getHotReloadTime())) {
                failures.set(0);

                return;
            }

            automaticHotReloadAttempts.incrementAndGet();

            if (log.isInfoEnabled())
                log.info("TLS hot reload started [transports=" + transports + ']');

            sslFactory.init();

            long hotReloadTime = sslFactory.getHotReloadTime();

            if (due(hotReloadTime)) {
                throw new IgniteException("Loaded certificates are still due for hot reload [hotReloadTime="
                    + new Date(hotReloadTime) + ']');
            }

            failures.set(0);

            if (log.isInfoEnabled())
                log.info("TLS hot reload succeeded [transports=" + transports + ']');

            logHotReloadTime(hotReloadTime);
        }

        /** @return Whether the loaded certificates have a hot reload time, and it has come. */
        private static boolean due(long hotReloadTime) {
            return hotReloadTime != 0 && hotReloadTime <= System.currentTimeMillis();
        }

        /** */
        private synchronized void reload() {
            manualReloadAttempts.incrementAndGet();

            ((AbstractSslContextFactory)factory).init();

            AbstractSslContextFactory sslFactory = hotReloadFactory();

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
