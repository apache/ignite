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

package org.apache.ignite.spi.tracing;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

/**
 * Set of tracing configuration parameters like sampling rate or included scopes.
 *
 * <p><b>WARNING:</b> This class MUST NOT be removed without considering PDS backward compatibility. Instances of this
 * class were stored in the distributed MetaStorage under the {@code "tr.config"} key. Since distributed MetaStorage values
 * can be written to the PDS, removing this class may prevent existing PDS from being deserialized.</p>
 *
 * @deprecated The Ignite Tracing is deprecated and subject to removal in a future release. Ignite Tracing has been
 * retired in favor of Ignite Performance Statistics and Ignite Metrics.
 */
@Deprecated(forRemoval = true)
public class TracingConfigurationParameters implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Min valid sampling rate with special meaning that span won't be created. */
    public static final double SAMPLING_RATE_NEVER = 0d;

    /** Max valid sampling rate with special meaning that span will be always created. */
    public static final double SAMPLING_RATE_ALWAYS = 1d;

    /**
     * Number between 0 and 1 that more or less reflects the probability of sampling a specific trace.
     * 0 and 1 have special meaning here, 0 means never 1 means always. Default value is 0 (never).
     */
    private final double samplingRate;

    /**
     * Set of {@link Scope} that defines which sub-traces will be included in a given trace.
     * In other words, if the child's span scope is equal to parent's scope
     * or it belongs to the parent's span included scopes, then the given child span will be attached to the current trace,
     * otherwise it'll be skipped.
     */
    private final Set<Scope> includedScopes;

    /**
     * Constructor.
     *
     * @param samplingRate Number between 0 and 1 that more or less reflects the probability of sampling specific trace.
     *  0 and 1 have special meaning here, 0 means never 1 means always. Default value is 0 (never).
     * @param includedScopes Set of {@link Scope} that defines which sub-traces will be included in given trace.
     *  In other words, if child's span scope is equals to parent's scope
     *  or it belongs to the parent's span included scopes, then given child span will be attached to the current trace,
     *  otherwise it'll be skipped.
     */
    private TracingConfigurationParameters(double samplingRate,
        Set<Scope> includedScopes) {
        this.samplingRate = samplingRate;
        this.includedScopes = Collections.unmodifiableSet(includedScopes);
    }

    /**
     * @return Number between 0 and 1 that more or less reflects the probability of sampling specific trace.
     * 0 and 1 have special meaning here, 0 means never 1 means always. Default value is 0 (never).
     * @deprecated The Ignite Tracing is deprecated and subject to removal in a future release. Ignite Tracing has been
     * retired in favor of Ignite Performance Statistics and Ignite Metrics.
     */
    @Deprecated(forRemoval = true)
    public double samplingRate() {
        return samplingRate;
    }

    /**
     * @return Set of {@link Scope} that defines which sub-traces will be included in given trace.
     * In other words, if child's span scope is equals to parent's scope
     * or it belongs to the parent's span included scopes, then given child span will be attached to the current trace,
     * otherwise it'll be skipped.
     * If no scopes are specified, empty set will be returned.
     * @deprecated The Ignite Tracing is deprecated and subject to removal in a future release. Ignite Tracing has been
     * retired in favor of Ignite Performance Statistics and Ignite Metrics.
     */
    @Deprecated(forRemoval = true)
    public @NotNull Set<Scope> includedScopes() {
        return Collections.unmodifiableSet(includedScopes);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TracingConfigurationParameters that = (TracingConfigurationParameters)o;

        if (Double.compare(that.samplingRate, samplingRate) != 0)
            return false;
        return includedScopes != null ? includedScopes.equals(that.includedScopes) : that.includedScopes == null;
    }

    /**
     * {@code TracingConfigurationParameters} builder.
     * @deprecated The Ignite Tracing is deprecated and subject to removal in a future release. Ignite Tracing has been
     * retired in favor of Ignite Performance Statistics and Ignite Metrics.
     */
    @Deprecated(forRemoval = true)
    @SuppressWarnings("PublicInnerClass") public static class Builder {
        /** Counterpart of {@code TracingConfigurationParameters} samplingRate. */
        private double samplingRate;

        /** Counterpart of {@code TracingConfigurationParameters} includedScopes. */
        private Set<Scope> includedScopes = Collections.emptySet();

        /**
         * Builder method that allows to set sampling rate.
         *
         * @param samplingRate Number between 0 and 1 that more or less reflects the probability of sampling specific trace.
         * 0 and 1 have special meaning here, 0 means never 1 means always. Default value is 0 (never).
         * @return {@code TracingConfigurationParameters} instance.
         * @deprecated The Ignite Tracing is deprecated and subject to removal in a future release. Ignite Tracing has been
         * retired in favor of Ignite Performance Statistics and Ignite Metrics.
         */
        @Deprecated(forRemoval = true)
        public @NotNull Builder withSamplingRate(double samplingRate) {
            if (samplingRate < SAMPLING_RATE_NEVER || samplingRate > SAMPLING_RATE_ALWAYS) {
                throw new IllegalArgumentException("Specified sampling rate=[" + samplingRate + "] has invalid value." +
                    " Should be between 0 and 1 including boundaries.");
            }
            this.samplingRate = samplingRate;

            return this;
        }

        /**
         * Builder method that allows to set included scopes.
         *
         * @param includedScopes Set of {@link Scope} that defines which sub-traces will be included in given trace.
         * In other words, if child's span scope is equals to parent's scope
         * or it belongs to the parent's span included scopes, then given child span will be attached to the current trace,
         * otherwise it'll be skipped.
         * @return {@code TracingConfigurationParameters} instance.
         * @deprecated The Ignite Tracing is deprecated and subject to removal in a future release. Ignite Tracing has been
         * retired in favor of Ignite Performance Statistics and Ignite Metrics.
         */
        @Deprecated(forRemoval = true)
        public @NotNull Builder withIncludedScopes(Set<Scope> includedScopes) {
            this.includedScopes = includedScopes == null ? Collections.emptySet() : includedScopes;

            return this;
        }

        /**
         * Builder's build() method.
         *
         * @return {@code TracingConfigurationParameters} instance.
         * @deprecated The Ignite Tracing is deprecated and subject to removal in a future release. Ignite Tracing has been
         * retired in favor of Ignite Performance Statistics and Ignite Metrics.
         */
        @Deprecated(forRemoval = true)
        public TracingConfigurationParameters build() {
            return new TracingConfigurationParameters(samplingRate, includedScopes);
        }
    }
}
