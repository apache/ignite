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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl;
import org.apache.ignite.internal.processors.tracing.Scope;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tracing configuration manager implementation that uses distributed meta storage
 * in order to store tracing configuration.
 */
public class GridTracingConfigurationManager implements TracingConfigurationManager {
    /** */
    private static final String TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY_PREFIX =
        DistributedMetaStorageImpl.IGNITE_INTERNAL_KEY_PREFIX + "tr.config.";

    /** Map with default configurations. */
    private static final Map<TracingConfigurationCoordinates, TracingConfigurationParameters> DEFAULT_CONFIGURATION_MAP;

    /** */
    public static final String WARNING_MSG_METASTORAGE_NOT_AVAILABLE =
        "Failed to save tracing configuration to meta storage. Meta storage is not available.";

    /** */
    public static final String WARNING_MSG_TO_RESET_CONFIG_METASTORAGE_NOT_AVAILABLE =
        "Failed to reset tracing configuration for coordinates=[%s] to default. Meta storage is not available.";

    /** */
    public static final String WARNING_MSG_TO_RESET_ALL_CONFIG_METASTORAGE_NOT_AVAILABLE =
        "Failed to reset tracing configuration for scope=[%s] to default. Meta storage is not available.";

    /** */
    public static final String WARNING_FAILED_TO_RETRIEVE_CONFIG_METASTORAGE_NOT_AVAILABLE =
        "Failed to retrieve tracing configuration. Meta storage is not available. Default value will be used.";

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
    }

    /** {@inheritDoc} */
    @Override public void set(
        @NotNull TracingConfigurationCoordinates coordinates,
        @NotNull TracingConfigurationParameters parameters)
    {
        DistributedMetaStorage metaStore = distributedMetaStorage(
            false,
            WARNING_MSG_METASTORAGE_NOT_AVAILABLE);

        String scopeSpecificKey = TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY_PREFIX + coordinates.scope().name();

        boolean configurationSuccessfullyUpdated = false;

        try {
            while (!configurationSuccessfullyUpdated) {
                HashMap<String, TracingConfigurationParameters> existingScopeSpecificTracingConfiguration =
                    metaStore.read(scopeSpecificKey);

                HashMap<String, TracingConfigurationParameters> updatedScopeSpecificTracingConfiguration =
                    existingScopeSpecificTracingConfiguration != null ?
                        new HashMap<>(existingScopeSpecificTracingConfiguration) : new
                        HashMap<>();

                updatedScopeSpecificTracingConfiguration.put(coordinates.label(), parameters);

                configurationSuccessfullyUpdated = metaStore.compareAndSet(
                    scopeSpecificKey,
                    existingScopeSpecificTracingConfiguration,
                    updatedScopeSpecificTracingConfiguration);
            }
        }
        catch (IgniteCheckedException e) {
            log.warning(WARNING_MSG_METASTORAGE_NOT_AVAILABLE, e);

            throw new IgniteException(WARNING_MSG_METASTORAGE_NOT_AVAILABLE, e);
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull TracingConfigurationParameters get(
        @NotNull TracingConfigurationCoordinates coordinates) {
        DistributedMetaStorage metaStore;

        try {
            metaStore = ctx.distributedMetastorage();
        }
        catch (Exception e) {
            LT.warn(log, WARNING_FAILED_TO_RETRIEVE_CONFIG_METASTORAGE_NOT_AVAILABLE);

            // If metastorage in not available — use scope specific default tracing configuration.
            return TracingConfigurationManager.super.get(coordinates);
        }

        if (metaStore == null) {
            LT.warn(log, WARNING_FAILED_TO_RETRIEVE_CONFIG_METASTORAGE_NOT_AVAILABLE);

            // If metastorage in not available — use scope specific default tracing configuration.
            return TracingConfigurationManager.super.get(coordinates);
        }

        String scopeSpecificKey = TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY_PREFIX + coordinates.scope().name();

        HashMap<String, TracingConfigurationParameters> scopeSpecificTracingConfiguration;

        try {
            scopeSpecificTracingConfiguration = metaStore.read(scopeSpecificKey);
        }
        catch (IgniteCheckedException e) {
            LT.warn(
                log,
                e,
                "Failed to retrieve tracing configuration. Default value will be used.",
                false,
                true);

            // In case of exception during retrieving configuration from metastorage — use scope specific default one.
            return TracingConfigurationManager.super.get(coordinates);
        }

        // If the configuration was not found — use scope specific default one.
        if (scopeSpecificTracingConfiguration == null)
            return TracingConfigurationManager.super.get(coordinates);

        // Retrieving scope + label specific tracing configuration.
        TracingConfigurationParameters lbBasedTracingConfiguration =
            scopeSpecificTracingConfiguration.get(coordinates.label());

        // If scope + label specific was found — use it.
        if (lbBasedTracingConfiguration != null)
            return lbBasedTracingConfiguration;

        // Retrieving scope specific tracing configuration.
        TracingConfigurationParameters rawScopedTracingConfiguration = scopeSpecificTracingConfiguration.get(null);

        // If scope specific was found — use it.
        if (rawScopedTracingConfiguration != null)
            return rawScopedTracingConfiguration;

        // If neither scope + label specific nor just scope specific configuration was found —
        // use scope specific default one.
        return TracingConfigurationManager.super.get(coordinates);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    @Override public @NotNull Map<TracingConfigurationCoordinates, TracingConfigurationParameters> getAll(
        @Nullable Scope scope
    ) {
        DistributedMetaStorage metaStore = distributedMetaStorage(
            true,
            WARNING_FAILED_TO_RETRIEVE_CONFIG_METASTORAGE_NOT_AVAILABLE);

        if (metaStore == null)
            return DEFAULT_CONFIGURATION_MAP;

        Map<TracingConfigurationCoordinates, TracingConfigurationParameters> res = new HashMap<>();

        for (Scope scopeToRetrieve : scope == null ? Scope.values() : new Scope[] {scope}) {
            String scopeSpecificKey = TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY_PREFIX + scopeToRetrieve.name();

            try {
                Map<String, TracingConfigurationParameters> scopeBasedTracingCfg = metaStore.read(scopeSpecificKey);

                if (scopeBasedTracingCfg == null) {
                    res.put(
                        new TracingConfigurationCoordinates.Builder(scopeToRetrieve).build(),
                        DEFAULT_CONFIGURATION_MAP.get(
                            new TracingConfigurationCoordinates.Builder(scopeToRetrieve).build()));

                    continue;
                }

                // Null keys encapsulates scope specific configuration.
                if (!scopeBasedTracingCfg.containsKey(null)) {
                    res.put(
                        new TracingConfigurationCoordinates.Builder(scopeToRetrieve).build(),
                        DEFAULT_CONFIGURATION_MAP.get(
                            new TracingConfigurationCoordinates.Builder(scopeToRetrieve).build()));
                }

                for (Map.Entry<String, TracingConfigurationParameters> entry :
                    ((Map<String, TracingConfigurationParameters>)metaStore.read(scopeSpecificKey)).entrySet()) {
                    res.put(
                        new TracingConfigurationCoordinates.Builder(scopeToRetrieve).
                            withLabel(entry.getKey()).build(),
                        entry.getValue());
                }

            }
            catch (IgniteCheckedException e) {
                LT.warn(log, "Failed to retrieve tracing configuration");
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void reset(@NotNull TracingConfigurationCoordinates coordinates) {
        DistributedMetaStorage metaStore = distributedMetaStorage(
            false,
            WARNING_MSG_TO_RESET_CONFIG_METASTORAGE_NOT_AVAILABLE,
            coordinates);

        String scopeSpecificKey = TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY_PREFIX + coordinates.scope().name();

        boolean configurationSuccessfullyUpdated = false;

        try {
            while (!configurationSuccessfullyUpdated) {
                HashMap<String, TracingConfigurationParameters> existingScopeSpecificTracingConfiguration =
                    metaStore.read(scopeSpecificKey);

                if (existingScopeSpecificTracingConfiguration == null) {
                    // Nothing to do.
                    return;
                }

                HashMap<String, TracingConfigurationParameters> updatedScopeSpecificTracingConfiguration =
                        new HashMap<>(existingScopeSpecificTracingConfiguration);

                if (coordinates.label() != null)
                    updatedScopeSpecificTracingConfiguration.remove(coordinates.label());
                else
                    updatedScopeSpecificTracingConfiguration.remove(null);

                configurationSuccessfullyUpdated = metaStore.compareAndSet(
                    scopeSpecificKey,
                    existingScopeSpecificTracingConfiguration,
                    updatedScopeSpecificTracingConfiguration);
            }
        }
        catch (IgniteCheckedException e) {
            log.warning(String.format(WARNING_MSG_TO_RESET_CONFIG_METASTORAGE_NOT_AVAILABLE, coordinates));

            throw new IgniteException(
                String.format(WARNING_MSG_TO_RESET_CONFIG_METASTORAGE_NOT_AVAILABLE, coordinates),
                e);
        }
    }

    /** {@inheritDoc} */
    @Override public void resetAll(@Nullable Scope scope) throws IgniteException {
        DistributedMetaStorage metaStore = distributedMetaStorage(
            false,
            WARNING_MSG_TO_RESET_ALL_CONFIG_METASTORAGE_NOT_AVAILABLE,
            scope);

        try {
            for (Scope scopeItem : scope == null ? Scope.values() : new Scope[] {scope})
                metaStore.remove(TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY_PREFIX + scopeItem.name());
        }
        catch (IgniteCheckedException e) {
            log.warning(String.format(WARNING_MSG_TO_RESET_ALL_CONFIG_METASTORAGE_NOT_AVAILABLE, scope));

            throw new IgniteException(
                String.format(WARNING_MSG_TO_RESET_ALL_CONFIG_METASTORAGE_NOT_AVAILABLE, scope),
                e);
        }
    }

    /**
     * Checks if metastore if available and return it.
     *
     * @param returnNull If {@code true} return null if distributed metastorage isn't available,
     *  otherwise throw {@code IgniteException}
     * @param warningMsg Warning message to be logged and used in {@code IgniteException}
     * @param warningMsgFormatArgs Arguments to warning message.
     *
     * @return Distributed metastorage if it's available.
     */
    private DistributedMetaStorage distributedMetaStorage(boolean returnNull, String warningMsg, Object...warningMsgFormatArgs) {
        DistributedMetaStorage metaStore;

        try {
            metaStore = ctx.distributedMetastorage();
        }
        catch (Exception e) {
            log.warning(String.format(warningMsg, warningMsgFormatArgs));

            if (returnNull)
                return null;
            else
                throw new IgniteException(String.format(warningMsg, warningMsgFormatArgs), e);
        }

        if (metaStore == null) {
            log.warning(String.format(warningMsg, warningMsgFormatArgs));

            if (returnNull)
                return null;
            else
                throw new IgniteException(String.format(warningMsg, warningMsgFormatArgs));
        }

        return metaStore;
    }
}
