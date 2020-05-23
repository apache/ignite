/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.tracing.configuration;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.tracing.Scope;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Allows to configure tracing, read the configuration and restore it to the defaults.
 */
public interface TracingConfigurationManager {
    // TODO: 04.05.20 After implementing GG-21041 and GG-21042 default TX config will include Scope.CACHE_GET, etc.
    /** Default transaction tracing configuration. */
    static final TracingConfigurationParameters DEFAULT_TX_CONFIGURATION =
        new TracingConfigurationParameters.Builder().
            withSamplingRate(0d).
            withIncludedScopes(Collections.emptySet()).
            build();

    /** Default exchange configuration. */
    static final TracingConfigurationParameters DEFAULT_EXCHANGE_CONFIGURATION =
        new TracingConfigurationParameters.Builder().
            withSamplingRate(0d).
            withIncludedScopes(Collections.emptySet()).
            build();

    /** Default discovery configuration. */
    static final TracingConfigurationParameters DEFAULT_DISCOVERY_CONFIGURATION =
        new TracingConfigurationParameters.Builder().
            withSamplingRate(0d).
            withIncludedScopes(Collections.emptySet()).
            build();

    /** Default communication configuration. */
    static final TracingConfigurationParameters DEFAULT_COMMUNICATION_CONFIGURATION =
        new TracingConfigurationParameters.Builder().
            withSamplingRate(0d).
            withIncludedScopes(Collections.emptySet()).
            build();

    /** Default noop configuration. */
    static final TracingConfigurationParameters NOOP_CONFIGURATION =
        new TracingConfigurationParameters.Builder().
            withSamplingRate(0d).
            withIncludedScopes(Collections.emptySet()).
            build();

    /**
     * Set new tracing configuration for the specific tracing coordinates (scope, label, etc.).
     * If tracing configuration with specified coordinates already exists it'll be overrided,
     * otherwise new one will be created.
     *
     * @param coordinates {@link TracingConfigurationCoordinates} Specific set of locators like {@link Scope} and label,
     *  that defines subset of traces and/or spans that'll use given configuration.
     * @param parameters {@link TracingConfigurationParameters} e.g. sampling rate, set of included scopes etc.
     * @throws IgniteException If failed to set tracing configuration.
     */
    void set(@NotNull TracingConfigurationCoordinates coordinates,
        @NotNull TracingConfigurationParameters parameters) throws IgniteException;

    /**
     * Get the most specific tracing parameters for the specified tracing coordinates (scope, label, etc.).
     * The most specific means:
     * <ul>
     *     <li>
     *         If there's tracing configuration that matches all tracing configuration attributes (scope and label) —
     *         it'll be returned.
     *     </li>
     *     <li>
     *         If there's no tracing configuration with specified label, or label wasn't specified —
     *         scope specific tracing configuration will be returned.
     *     </li>
     *     <li>
     *         If there's no tracing configuration with specified scope —
     *         default scope specific configuration will be returned.
     *     </li>
     * </ul>
     *
     * @param coordinates {@link TracingConfigurationCoordinates} Specific set of locators like {@link Scope} and label
     *  that defines a subset of traces and/or spans that'll use given configuration.
     * @return {@link TracingConfigurationParameters} instance.
     * @throws IgniteException If failed to get tracing configuration.
     */
    default @NotNull TracingConfigurationParameters get(
        @NotNull TracingConfigurationCoordinates coordinates) throws IgniteException
    {
        switch (coordinates.scope()) {
            case TX: {
                return DEFAULT_TX_CONFIGURATION;
            }

            case EXCHANGE: {
                return DEFAULT_EXCHANGE_CONFIGURATION;
            }

            case DISCOVERY: {
                return DEFAULT_DISCOVERY_CONFIGURATION;
            }

            case COMMUNICATION: {
                return DEFAULT_COMMUNICATION_CONFIGURATION;
            }

            default: {
                return NOOP_CONFIGURATION;
            }
        }
    }

    /**
     * List all pairs of tracing configuration coordinates and tracing configuration parameters
     * or list all pairs of tracing configuration and parameters for the specific scope.
     *
     * @param scope Nullable scope of tracing configuration to be retrieved.
     *  If null - all configuration will be returned.
     * @return The whole set of tracing configuration.
     * @throws IgniteException If failed to get tracing configuration.
     */
    @NotNull Map<TracingConfigurationCoordinates, TracingConfigurationParameters> getAll(
        @Nullable Scope scope) throws IgniteException;

    /**
     * Reset tracing configuration for the specific tracing coordinates (scope, label, etc.) to default values.
     * Please pay attention, that there's no default values for label specific coordinates,
     * so such kinds of configurations will be removed.
     *
     * @param coordinates {@link TracingConfigurationCoordinates} specific set of locators like {@link Scope} and label
     *  that defines a subset of traces and/or spans that will be reset.
     *  @throws IgniteException If failed to reset tracing configuration.
     */
    void reset(@NotNull TracingConfigurationCoordinates coordinates) throws IgniteException;

    /**
     * Reset tracing configuration for the specific scope, or all tracing configurations if scope not specified.
     *
     * @param scope {@link Scope} that defines a set of applicable tracing configurations.
     * @throws IgniteException If failed to reset tracing configuration.
     */
    void resetAll(@Nullable Scope scope) throws IgniteException;
}
