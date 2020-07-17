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

package org.apache.ignite.internal.commandline.configuration;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task that will collect and update tracing configuration.
 */
@GridInternal
@GridVisorManagementTask
public class VisorTracingConfigurationTask
    extends VisorOneNodeTask<VisorTracingConfigurationTaskArg, VisorTracingConfigurationTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorTracingConfigurationJob job(VisorTracingConfigurationTaskArg arg) {
        return new VisorTracingConfigurationJob(arg, debug);
    }

    /**
     * Job that will collect and update tracing configuration.
     */
    private static class VisorTracingConfigurationJob extends VisorJob<VisorTracingConfigurationTaskArg, VisorTracingConfigurationTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorTracingConfigurationJob(VisorTracingConfigurationTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected @NotNull VisorTracingConfigurationTaskResult run(
            VisorTracingConfigurationTaskArg arg) throws IgniteException {
            switch (arg.operation()) {
                case GET_ALL:
                    return getAll(arg.scope());

                case GET:
                    return get(arg.scope(), arg.label());

                case RESET:
                    return reset(arg.scope(), arg.label());

                case RESET_ALL:
                    return resetAll(arg.scope());

                case SET:
                    return set(arg.scope(), arg.label(), arg.samplingRate(), arg.includedScopes());

                default: {
                    // We should never get here.
                    assert false : "Unexpected tracing configuration argument [arg= " + arg + ']';

                    return getAll(null); // Just in case.
                }
            }
        }

        /**
         * Get tracing configuration.
         *
         * @param scope Nullable scope of tracing configuration to be retrieved.
         *  If null - all configuration will be returned.
         * @return Tracing configuration as {@link VisorTracingConfigurationTaskResult} instance.
         */
        private @NotNull VisorTracingConfigurationTaskResult getAll(@Nullable Scope scope) {
            Map<TracingConfigurationCoordinates, TracingConfigurationParameters> cfg =
                ignite.tracingConfiguration().getAll(scope);

            VisorTracingConfigurationTaskResult res = new VisorTracingConfigurationTaskResult();

            for (Map.Entry<TracingConfigurationCoordinates, TracingConfigurationParameters> entry: cfg.entrySet())
                res.add(entry.getKey(), entry.getValue());

            return res;
        }

        /**
         * Get scope specific and optionally label specific tracing configuration.
         *
         * @param scope Scope.
         * @param lb Label
         * @return Scope specific and optionally label specific tracing configuration as
         *  {@link VisorTracingConfigurationTaskResult} instance.
         */
        private @NotNull VisorTracingConfigurationTaskResult get(
            @NotNull Scope scope,
            @Nullable String lb)
        {
            TracingConfigurationCoordinates coordinates =
                new TracingConfigurationCoordinates.Builder(scope).withLabel(lb).build();

            TracingConfigurationParameters updatedParameters =
                ignite.tracingConfiguration().get(
                    new TracingConfigurationCoordinates.Builder(scope).withLabel(lb).build());

            VisorTracingConfigurationTaskResult res = new VisorTracingConfigurationTaskResult();

            res.add(coordinates, updatedParameters);

            return res;
        }

        /**
         * Reset scope specific and optionally label specific tracing configuration.
         *
         * @param scope Scope.
         * @param lb Label.
         * @return Scope based configuration that was partly of fully reseted as
         *  {@link VisorTracingConfigurationTaskResult} instance.
         */
        private @NotNull VisorTracingConfigurationTaskResult reset(
            @NotNull Scope scope,
            @Nullable String lb)
        {
            ignite.tracingConfiguration().reset(
                new TracingConfigurationCoordinates.Builder(scope).withLabel(lb).build());

            return getAll(scope);
        }

        /**
         * Reset tracing configuration, or optionally scope specific tracing configuration.
         *
         * @param scope Scope.
         * @return Tracing configuration as {@link VisorTracingConfigurationTaskResult} instance.
         */
        private @NotNull VisorTracingConfigurationTaskResult resetAll(@Nullable Scope scope) {
            ignite.tracingConfiguration().resetAll(scope);

            return getAll(scope);
        }

        /**
         * Set new tracing configuration.
         *
         * @param scope Scope.
         * @param lb Label.
         * @param samplingRate Sampling rate.
         * @param includedScopes Set of included scopes.
         * @return Scope based configuration that was partly of fully updated as
         *          *  {@link VisorTracingConfigurationTaskResult} instance.
         */
        private @NotNull VisorTracingConfigurationTaskResult set(
            @NotNull Scope scope,
            @Nullable String lb,
            @Nullable Double samplingRate,
            @Nullable Set<Scope> includedScopes)
        {
            TracingConfigurationCoordinates coordinates =
                new TracingConfigurationCoordinates.Builder(scope).withLabel(lb).build();

            TracingConfigurationParameters.Builder parametersBuilder = new TracingConfigurationParameters.Builder();

            if (samplingRate != null)
                parametersBuilder.withSamplingRate(samplingRate);

            if (includedScopes != null)
                parametersBuilder.withIncludedScopes(includedScopes);

            ignite.tracingConfiguration().set(coordinates, parametersBuilder.build());

            return getAll(scope);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorTracingConfigurationJob.class, this);
        }
    }
}
