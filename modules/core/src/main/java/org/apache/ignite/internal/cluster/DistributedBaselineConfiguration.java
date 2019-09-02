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

import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.configuration.distributed.DistributePropertyListener;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.NotNull;

import static java.lang.String.format;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedBooleanProperty;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty.detachedLongProperty;
import static org.apache.ignite.internal.util.IgniteUtils.isLocalNodeCoordinator;

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

    /** Message of baseline auto-adjust parameter was changed from default. */
    private static final String DEFAULT_PROPERTY_UPDATE_MESSAGE =
        "Baseline parameter '%s' was changed from default value='%s' to '%s'";

    /** Message of baseline auto-adjust parameter was changed. */
    private static final String PROPERTY_UPDATE_MESSAGE =
        "Baseline parameter '%s' was changed from '%s' to '%s'";

    /** */
    private volatile long dfltTimeout;

    /** Default auto-adjust enable/disable. */
    private volatile boolean dfltEnabled;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** Value of manual baseline control or auto adjusting baseline. */
    private final DistributedBooleanProperty baselineAutoAdjustEnabled =
        detachedBooleanProperty("baselineAutoAdjustEnabled");

    /**
     * Value of time which we would wait before the actual topology change since last discovery event(node join/exit).
     */
    private final DistributedLongProperty baselineAutoAdjustTimeout =
        detachedLongProperty("baselineAutoAdjustTimeout");

    /**
     * @param isp Subscription processor.
     * @param ctx Kernal context.
     */
    public DistributedBaselineConfiguration(
        GridInternalSubscriptionProcessor isp,
        GridKernalContext ctx,
        IgniteLogger log) {
        this.ctx = ctx;
        this.log = log;

        boolean persistenceEnabled = ctx.config() != null && CU.isPersistenceEnabled(ctx.config());

        dfltTimeout = persistenceEnabled ? DEFAULT_PERSISTENCE_TIMEOUT : DEFAULT_IN_MEMORY_TIMEOUT;
        dfltEnabled = getBoolean(IGNITE_BASELINE_AUTO_ADJUST_ENABLED, !persistenceEnabled);

        isp.registerDistributedConfigurationListener(
            dispatcher -> {
                baselineAutoAdjustEnabled.addListener(makeUpdateListener(dfltEnabled));
                baselineAutoAdjustTimeout.addListener(makeUpdateListener(dfltTimeout));

                dispatcher.registerProperty(baselineAutoAdjustEnabled);
                dispatcher.registerProperty(baselineAutoAdjustTimeout);
            }
        );
    }

    /**
     * @param defaultVal Default value from which property can be changed in first time.
     * @param <T> Type of property value.
     * @return Update property listener.
     */
    @NotNull private <T> DistributePropertyListener<T> makeUpdateListener(Object defaultVal) {
        return (name, oldVal, newVal) -> {
            if (!Objects.equals(oldVal, newVal)) {
                if (oldVal == null)
                    log.info(format(DEFAULT_PROPERTY_UPDATE_MESSAGE, name, defaultVal, newVal));
                else
                    log.info(format(PROPERTY_UPDATE_MESSAGE, name, oldVal, newVal));
            }
        };
    }

    /**
     * Called when cluster performing activation.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void onActivate() throws IgniteCheckedException {
        if (baselineAutoAdjustEnabled.get() == null && isLocalNodeCoordinator(ctx.discovery())) {
            boolean dfltEnableVal = getBoolean(IGNITE_BASELINE_AUTO_ADJUST_ENABLED, isCurrentBaselineNew());

            //Set default enable flag to cluster only if it is true.
            if (dfltEnableVal)
                baselineAutoAdjustEnabled.propagate(dfltEnableVal);
        }

        if (isLocalNodeCoordinator(ctx.discovery())) {
            log.info(format(AUTO_ADJUST_CONFIGURED_MESSAGE,
                (isBaselineAutoAdjustEnabled() ? "enabled" : "disabled"),
                getBaselineAutoAdjustTimeout()
            ));
        }
    }

    /**
     * @return {@code true} if current baseline is new.(It is first activation for cluster.)
     */
    private boolean isCurrentBaselineNew() {
        BaselineTopology baselineTop = ctx.state().clusterState().baselineTopology();

        return baselineTop != null && baselineTop.isNewTopology();
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
        return this.baselineAutoAdjustEnabled.propagateAsync(baselineAutoAdjustEnabled);
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
