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

package org.apache.ignite.internal.commandline.tracing.configuration;

import java.util.Collections;
import java.util.Set;
import org.apache.ignite.internal.commandline.configuration.VisorTracingConfigurationItem;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.spi.tracing.Scope;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This class contains all possible arguments after parsing tracing-configuration command input.
 */
 public final class TracingConfigurationArguments extends VisorTracingConfigurationItem {
    /** */
    private static final long serialVersionUID = 0L;

    /** Command. */
    private TracingConfigurationSubcommand cmd;

    /**
     * Default constructor.
     */
    public TracingConfigurationArguments() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cmd Command
     * @param scope Specify the {@link Scope} of a trace's root span
     *  to which some specific tracing configuration will be applied.
     * @param lb Specifies the label of a traced operation. It's an optional attribute.
     * @param samplingRate Number between 0 and 1 that more or less reflects the probability of sampling specific trace.
     *  0 and 1 have special meaning here, 0 means never 1 means always. Default value is 0 (never).
     * @param includedScopes Set of {@link Scope} that defines which sub-traces will be included in given trace.
     *  In other words, if child's span scope is equals to parent's scope
     *  or it belongs to the parent's span included scopes, then given child span will be attached to the current trace,
     *  otherwise it'll be skipped.
     *  See {@link Span#isChainable(Scope)} for more details.
     */
    private TracingConfigurationArguments(
        TracingConfigurationSubcommand cmd,
        Scope scope,
        String lb,
        double samplingRate,
        Set<Scope> includedScopes
    ) {
        super(
            scope,
            lb,
            samplingRate,
            includedScopes);

        this.cmd = cmd;
    }

    /**
     * @return Command.
     */
    public TracingConfigurationSubcommand command() {
        return cmd;
    }

    /**
     * Builder of {@link TracingConfigurationArguments}.
     */
    @SuppressWarnings("PublicInnerClass") public static class Builder {
        /** Counterpart of {@code TracingConfigurationArguments}'s command. */
        private TracingConfigurationSubcommand cmd;

        /** Counterpart of {@code TracingConfigurationArguments}'s scope. */
        private Scope scope;

        /** Counterpart of {@code TracingConfigurationArguments}'s label. */
        private String lb;

        /** Counterpart of {@code TracingConfigurationArguments}'s samplingRate. */
        private double samplingRate;

        /** Counterpart of {@code TracingConfigurationArguments}'s includedScopes. */
        private Set<Scope> includedScopes;

        /**
         * Constructor.
         *
         * @param cmd {@code TracingConfigurationSubcommand} command.
         */
        public Builder(TracingConfigurationSubcommand cmd) {
            this.cmd = cmd;
        }

        /**
         * Builder method that allows to set scope.
         * @param scope {@link Scope} of a trace's root span
         *  to which some specific tracing configuration will be applied.
         * @return Builder.
         */
        public @NotNull Builder withScope(Scope scope) {
            this.scope = scope;

            return this;
        }

        /**
         * Builder method that allows to set sampling rate.
         *
         * @param samplingRate Number between 0 and 1 that more or less reflects the probability
         *  of sampling specific trace.
         *  0 and 1 have special meaning here, 0 means never 1 means always. Default value is 0 (never).
         * @return Builder.
         */
        public @NotNull Builder withSamplingRate(double samplingRate) {
            this.samplingRate = samplingRate;

            return this;
        }

        /**
         * Builder method that allows to set optional label attribute.
         *
         * @param lb Label of traced operation. It's an optional attribute.
         * @return Builder
         */
        public @NotNull Builder withLabel(@Nullable String lb) {
            this.lb = lb;

            return this;
        }

        /**
         * Builder method that allows to set included scopes.
         *
         * @param includedScopes Set of {@link Scope} that defines which sub-traces will be included in given trace.
         * In other words, if child's span scope is equals to parent's scope
         * or it belongs to the parent's span included scopes, then given child span will be attached to the current trace,
         * otherwise it'll be skipped.
         * See {@link Span#isChainable(Scope)} for more details.
         * @return Builder
         */
        @SuppressWarnings("UnusedReturnValue")
        public @NotNull Builder withIncludedScopes(Set<Scope> includedScopes) {
            this.includedScopes = includedScopes == null ? Collections.emptySet() : includedScopes;

            return this;
        }

        /**
         * Builder's build method that produces {@link TracingConfigurationArguments}.
         *
         * @return {@code TracingConfigurationArguments} instance.
         */
        public TracingConfigurationArguments build() {
            return new TracingConfigurationArguments(
                cmd,
                scope,
                lb,
                samplingRate,
                includedScopes);
        }
    }
}
