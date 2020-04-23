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

package org.apache.ignite.internal.cluster;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.configuration.distributed.DistributePropertyListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationLifecycleListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedPropertyDispatcher;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;

import static java.lang.String.format;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedBooleanProperty;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty.detachedLongProperty;

/**
 * Distributed baseline configuration.
 */
public class DistributedBaselineConfiguration {
    /** Default auto-adjust timeout for persistence grid. */
    private static final int DEFAULT_PERSISTENCE_TIMEOUT = 5 * 60_000;

    /** Default auto-adjust timeout for in-memory grid. */
    private static final int DEFAULT_IN_MEMORY_TIMEOUT = 0;

    /** Message of baseline auto-adjust configuration. */
    private static final String AUTO_ADJUST_CONFIGURED_MESSAGE = "Baseline auto-adjust is '%s' with timeout='%d' ms";

    /** Message of baseline auto-adjust parameter was changed. */
    private static final String PROPERTY_UPDATE_MESSAGE =
        "Baseline parameter '%s' was changed from '%s' to '%s'";

    /** */
    private volatile long dfltTimeout;

    /** Default auto-adjust enable/disable. */
    private volatile boolean dfltEnabled;

    /** */
    private final IgniteLogger log;

    /** Value of manual baseline control or auto adjusting baseline. */
    private final DistributedChangeableProperty<Boolean> baselineAutoAdjustEnabled =
        detachedBooleanProperty("baselineAutoAdjustEnabled");

    /**
     * Value of time which we would wait before the actual topology change since last discovery event(node join/exit).
     */
    private final DistributedChangeableProperty<Long> baselineAutoAdjustTimeout =
        detachedLongProperty("baselineAutoAdjustTimeout");

    /**
     * @param isp Subscription processor.
     * @param ctx Kernal context.
     */
    public DistributedBaselineConfiguration(
        GridInternalSubscriptionProcessor isp,
        GridKernalContext ctx,
        IgniteLogger log) {
        this.log = log;

        boolean persistenceEnabled = ctx.config() != null && CU.isPersistenceEnabled(ctx.config());

        dfltTimeout = persistenceEnabled ? DEFAULT_PERSISTENCE_TIMEOUT : DEFAULT_IN_MEMORY_TIMEOUT;
        dfltEnabled = !persistenceEnabled;

        isp.registerDistributedConfigurationListener(
            new DistributedConfigurationLifecycleListener() {
                @Override public void onReadyToRegister(DistributedPropertyDispatcher dispatcher) {
                    baselineAutoAdjustEnabled.addListener(makeUpdateListener());
                    baselineAutoAdjustTimeout.addListener(makeUpdateListener());

                    dispatcher.registerProperties(baselineAutoAdjustEnabled, baselineAutoAdjustTimeout);
                }

                @Override public void onReadyToWrite() {
                    setDefaultValue(baselineAutoAdjustEnabled, dfltEnabled, log);
                    setDefaultValue(baselineAutoAdjustTimeout, dfltTimeout, log);
                }
            }
        );
    }

    /**
     * @param property Property which value should be set.
     * @param value Default value.
     * @param log Logger.
     * @param <T> Property type.
     */
    private <T extends Serializable> void setDefaultValue(DistributedProperty<T> property, T value, IgniteLogger log) {
        if (property.get() == null) {
            try {
                property.propagateAsync(null, value)
                    .listen((IgniteInClosure<IgniteInternalFuture<?>>)future -> {
                        if (future.error() != null)
                            log.error("Cannot set default value of '" + property.getName() + '\'', future.error());
                    });
            }
            catch (IgniteCheckedException e) {
                log.error("Cannot initiate setting default value of '" + property.getName() + '\'', e);
            }
        }
    }

    /**
     * @param <T> Type of property value.
     * @return Update property listener.
     */
    @NotNull private <T> DistributePropertyListener<T> makeUpdateListener() {
        return (name, oldVal, newVal) -> {
            if (!Objects.equals(oldVal, newVal))
                log.info(format(PROPERTY_UPDATE_MESSAGE, name, oldVal, newVal));
        };
    }

    /**
     * Called when cluster performing activation.
     */
    public void onActivate() throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info(format(AUTO_ADJUST_CONFIGURED_MESSAGE,
                (isBaselineAutoAdjustEnabled() ? "enabled" : "disabled"),
                getBaselineAutoAdjustTimeout()
            ));
    }

    /**
     * @return Value of manual baseline control or auto adjusting baseline.
     */
    public boolean isBaselineAutoAdjustEnabled() {
        return baselineAutoAdjustEnabled.getOrDefault(dfltEnabled);
    }

    /**
     * @param baselineAutoAdjustEnabled Value of manual baseline control or auto adjusting baseline.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> updateBaselineAutoAdjustEnabledAsync(boolean baselineAutoAdjustEnabled)
        throws IgniteCheckedException {
        return this.baselineAutoAdjustEnabled.propagateAsync(!baselineAutoAdjustEnabled, baselineAutoAdjustEnabled);
    }

    /**
     * @return Value of time which we would wait before the actual topology change since last discovery event(node
     * join/exit).
     */
    public long getBaselineAutoAdjustTimeout() {
        return baselineAutoAdjustTimeout.getOrDefault(dfltTimeout);
    }

    /**
     * @param baselineAutoAdjustTimeout Value of time which we would wait before the actual topology change since last
     * discovery event(node join/exit).
     * @throws IgniteCheckedException If failed.
     */
    public GridFutureAdapter<?> updateBaselineAutoAdjustTimeoutAsync(
        long baselineAutoAdjustTimeout) throws IgniteCheckedException {
        return this.baselineAutoAdjustTimeout.propagateAsync(baselineAutoAdjustTimeout);
    }
}
