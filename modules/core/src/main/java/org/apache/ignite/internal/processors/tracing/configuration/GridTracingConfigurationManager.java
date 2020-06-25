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

package org.apache.ignite.internal.processors.tracing.configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tracing configuration manager implementation that uses distributed meta storage
 * in order to store tracing configuration.
 */
public class GridTracingConfigurationManager implements TracingConfigurationManager {
    /** Map with default configurations. */
    private static final Map<TracingConfigurationCoordinates, TracingConfigurationParameters> DEFAULT_CONFIGURATION_MAP;

    /** */
    public static final String WARNING_MSG_TRACING_CONFIG_UPDATE_FAILED_COORDINATES =
        "Failed to update tracing configuration for coordinates=[%s].";

    /** */
    public static final String WARNING_MSG_TRACING_CONFIG_UPDATE_FAILED_SCOPE =
        "Failed to update tracing configuration for scope=[%s].";

    /** Tracing configuration distributed property. */
    private final DistributedTracingConfiguration distributedTracingConfiguration =
        DistributedTracingConfiguration.detachedProperty();

    /** Tracing configuration. */
    private volatile Map<TracingConfigurationCoordinates, TracingConfigurationParameters> tracingConfiguration =
        DEFAULT_CONFIGURATION_MAP;

    /** Mutex for updating local tracing configuration. */
    @GridToStringExclude
    private final Object mux = new Object();

    static {
        Map<TracingConfigurationCoordinates, TracingConfigurationParameters> tmpDfltConfigurationMap = new HashMap<>();

        tmpDfltConfigurationMap.put(
            new TracingConfigurationCoordinates.Builder(Scope.TX).build(),
            TracingConfigurationManager.DEFAULT_TX_CONFIGURATION);

        tmpDfltConfigurationMap.put(
            new TracingConfigurationCoordinates.Builder(Scope.COMMUNICATION).build(),
            TracingConfigurationManager.DEFAULT_COMMUNICATION_CONFIGURATION);

        tmpDfltConfigurationMap.put(
            new TracingConfigurationCoordinates.Builder(Scope.EXCHANGE).build(),
            TracingConfigurationManager.DEFAULT_EXCHANGE_CONFIGURATION);

        tmpDfltConfigurationMap.put(
            new TracingConfigurationCoordinates.Builder(Scope.DISCOVERY).build(),
            TracingConfigurationManager.DEFAULT_DISCOVERY_CONFIGURATION);

        DEFAULT_CONFIGURATION_MAP = Collections.unmodifiableMap(tmpDfltConfigurationMap);
    }

    /** Kernal context. */
    @GridToStringExclude
    protected final GridKernalContext ctx;

    /** Grid logger. */
    @GridToStringExclude
    protected final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public GridTracingConfigurationManager(@NotNull GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(dispatcher -> {
            distributedTracingConfiguration.addListener((name, oldVal, newVal) -> {
                synchronized (mux) {
                    if (log.isDebugEnabled())
                        log.debug("Tracing configuration was updated [oldVal= " + oldVal + ", newVal=" + newVal + "]");

                    if (newVal != null && !newVal.isEmpty())
                        tracingConfiguration = newVal;
                }
            });

            dispatcher.registerProperty(distributedTracingConfiguration);
        });
    }

    /** {@inheritDoc} */
    @Override public void set(
        @NotNull TracingConfigurationCoordinates coordinates,
        @NotNull TracingConfigurationParameters parameters)
    {
        HashMap<TracingConfigurationCoordinates, TracingConfigurationParameters> newTracingConfiguration =
            new HashMap<>(tracingConfiguration);

        newTracingConfiguration.put(coordinates, parameters);

        try {
            distributedTracingConfiguration.propagate(newTracingConfiguration);
        }
        catch (IgniteCheckedException e) {
            String warningMsg = String.format(WARNING_MSG_TRACING_CONFIG_UPDATE_FAILED_COORDINATES, coordinates);

            log.warning(warningMsg, e);

            throw new IgniteException(warningMsg, e);
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationParameters get(@NotNull TracingConfigurationCoordinates coordinates) {
        TracingConfigurationParameters coordinateSpecificParameters = tracingConfiguration.get(coordinates);

        // If parameters for the specified coordinates (both scope and label) were not found use only scope specific one.
        // If there are no custom scope specific parameters, default one will be used.
        return coordinateSpecificParameters == null ?
            tracingConfiguration.get(new TracingConfigurationCoordinates.Builder(coordinates.scope()).build()) :
            coordinateSpecificParameters;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Override public @NotNull Map<TracingConfigurationCoordinates, TracingConfigurationParameters> getAll(
        @Nullable Scope scope) {
        return scope != null ?
            tracingConfiguration.entrySet().stream().
                filter(e -> e.getKey().scope() == scope).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)) :
            tracingConfiguration;
    }

    /** {@inheritDoc} */
    @Override public void reset(@NotNull TracingConfigurationCoordinates coordinates) {
        HashMap<TracingConfigurationCoordinates, TracingConfigurationParameters> newTracingConfiguration =
            new HashMap<>(tracingConfiguration);

        if (coordinates.label() != null)
            newTracingConfiguration.remove(coordinates);
        else
            newTracingConfiguration.put(coordinates, DEFAULT_CONFIGURATION_MAP.get(new TracingConfigurationCoordinates.Builder(coordinates.scope()).build()));

        try {
            distributedTracingConfiguration.propagate(newTracingConfiguration);
        }
        catch (IgniteCheckedException e) {
            String warningMsg = String.format(WARNING_MSG_TRACING_CONFIG_UPDATE_FAILED_COORDINATES, coordinates);

            log.warning(warningMsg, e);

            throw new IgniteException(warningMsg, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void resetAll(@Nullable Scope scope) throws IgniteException {
        HashMap<TracingConfigurationCoordinates, TracingConfigurationParameters> newTracingConfiguration;

        if (scope != null) {
            newTracingConfiguration = new HashMap<>(tracingConfiguration.entrySet().stream().
                filter(e -> e.getKey().scope() != scope).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

            TracingConfigurationCoordinates scopeSpecificCoordinates =
                new TracingConfigurationCoordinates.Builder(scope).build();

            newTracingConfiguration.put(scopeSpecificCoordinates,
                DEFAULT_CONFIGURATION_MAP.get(scopeSpecificCoordinates));
        }
        else
            newTracingConfiguration = new HashMap<>(DEFAULT_CONFIGURATION_MAP);

        try {
            distributedTracingConfiguration.propagate(newTracingConfiguration);
        }
        catch (IgniteCheckedException e) {
            String warningMsg = String.format(WARNING_MSG_TRACING_CONFIG_UPDATE_FAILED_SCOPE, scope);

            log.warning(warningMsg, e);

            throw new IgniteException(warningMsg, e);
        }
    }
}
