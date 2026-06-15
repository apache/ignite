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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Cache operation context.
 */
public class CacheOperationContext implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final CacheOperationContext INSTANCE = new Builder().build();

    /** Skip store flag. */
    @GridToStringInclude
    private final boolean skipStore;

    /** Skip read through flag. */
    @GridToStringInclude
    private final boolean skipReadThrough;

    /** No retries flag. */
    @GridToStringInclude
    private final boolean noRetries;

    /** Recovery flag. */
    private final boolean recovery;

    /** Read-repair strategy.*/
    private final @Nullable ReadRepairStrategy readRepairStrategy;

    /** Keep binary flag. */
    private final boolean keepBinary;

    /** Expiry policy. */
    private final @Nullable ExpiryPolicy expiryPlc;

    /** Data center Id. */
    private final @Nullable Byte dataCenterId;

    /** Application attributes. */
    private final @Nullable Map<String, String> appAttrs;

    /** Handle binary in interceptor operation flag. */
    private final boolean handleBinaryInInterceptor;

    /**
     * @param skipStore                  Skip store flag.
     * @param skipReadThrough            Skip read-through cache store flag.
     * @param keepBinary                 Keep binary flag.
     * @param expiryPlc                  Expiry policy.
     * @param dataCenterId               Data center id.
     * @param readRepairStrategy         Read-repair strategy.
     * @param appAttrs                   Application attributes.
     * @param handleBinaryInInterceptor  Handle binary in interceptor operation flag.
     */
    private CacheOperationContext(
        boolean skipStore,
        boolean skipReadThrough,
        boolean keepBinary,
        @Nullable ExpiryPolicy expiryPlc,
        boolean noRetries,
        @Nullable Byte dataCenterId,
        boolean recovery,
        @Nullable ReadRepairStrategy readRepairStrategy,
        @Nullable Map<String, String> appAttrs,
        boolean handleBinaryInInterceptor
    ) {
        this.skipStore = skipStore;
        this.skipReadThrough = skipReadThrough;
        this.keepBinary = keepBinary;
        this.expiryPlc = expiryPlc;
        this.noRetries = noRetries;
        this.dataCenterId = dataCenterId;
        this.recovery = recovery;
        this.readRepairStrategy = readRepairStrategy;
        this.appAttrs = appAttrs;
        this.handleBinaryInInterceptor = handleBinaryInInterceptor;
    }

    /**
     * Helper.
     */
    public static CacheOperationContext instance() {
        return INSTANCE;
    }

    /**
     * @return keepBinary flag.
     */
    public boolean isKeepBinary() {
        return keepBinary;
    }

    /** Context with keepBinary flag. */
    public CacheOperationContext withKeepBinary() {
        return builder(this).keepBinary(true).build();
    }

    /**
     * Gets data center ID.
     *
     * @return Datacenter ID.
     */
    @Nullable public Byte dataCenterId() {
        return dataCenterId;
    }

    /** Context with dataCenterId. */
    public CacheOperationContext withDataCenterId(Byte dataCenterId) {
        return builder(this).dataCenterId(dataCenterId).build();
    }

    /**
     * @return Partition recover flag.
     */
    public boolean recovery() {
        return recovery;
    }

    /** Context with recovery flag. */
    public CacheOperationContext withRecovery() {
        return builder(this).recovery(true).build();
    }

    /**
     * @return Read Repair strategy.
     */
    @Nullable public ReadRepairStrategy readRepairStrategy() {
        return readRepairStrategy;
    }

    /** Context with read repair strategy. */
    public CacheOperationContext withReadRepairStrategy(ReadRepairStrategy strategy) {
        return builder(this).readRepairStrategy(strategy).build();
    }

    /**
     * @return No retries flag.
     */
    public boolean noRetries() {
        return noRetries;
    }

    /** Context with noRetries flag. */
    public CacheOperationContext withNoRetries() {
        return builder(this).noRetries(true).build();
    }

    /**
     * @return Application attributes.
     */
    @Nullable public Map<String, String> applicationAttributes() {
        return appAttrs;
    }

    /** Context with application attributes. */
    public CacheOperationContext withApplicationAttributes(Map<String, String> attrs) {
        return builder(this).applicationAttributes(attrs).build();
    }

    /**
     * @return Skip store.
     */
    public boolean skipStore() {
        return skipStore;
    }

    /** Context with skipStore flag. */
    public CacheOperationContext withSkipStore() {
        return builder(this).skipStore(true).build();
    }

    /**
     * @return Skip read-through cache store.
     */
    public boolean skipReadThrough() {
        return skipReadThrough;
    }

    /** Context with {@link CacheOperationContext#skipReadThrough} flag. */
    public CacheOperationContext withSkipReadThrough() {
        return builder(this).skipReadThrough(true).build();
    }

    /** @return Whether to handle binary in interceptor. */
    public boolean handleBinaryInInterceptor() {
        return handleBinaryInInterceptor;
    }

    /** Context with {@link CacheOperationContext#handleBinaryInInterceptor} flag. */
    public CacheOperationContext withHandleBinaryInInterceptor() {
        return builder(this).handleBinaryInInterceptor(true).build();
    }

    /**
     * @return {@link ExpiryPolicy} associated with this projection.
     */
    @Nullable public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /** Context with {@link CacheOperationContext#expiryPlc}. */
    public CacheOperationContext withExpiryPolicy(ExpiryPolicy plc) {
        return builder(this).expiryPolicy(plc).build();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheOperationContext.class, this);
    }

    /**
     * Creates the builder from existing context.
     *
     * @return Builder for cache operations context.
     */
    public static Builder builder(CacheOperationContext ctx) {
        return new Builder(ctx);
    }

    /**
     * Creates the builder for cache operations context.
     *
     * @return Builder for cache operations context.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Cache operations context builder. */
    public static class Builder {
        /** Skip store. */
        @GridToStringInclude
        private boolean skipStore;

        /** Skip read through. */
        @GridToStringInclude
        private boolean skipReadThrough;

        /** No retries flag. */
        @GridToStringInclude
        private boolean noRetries;

        /** Recovery flag. */
        private boolean recovery;

        /** Read-repair strategy. */
        private ReadRepairStrategy readRepairStrategy;

        /** Keep binary flag. */
        private boolean keepBinary;

        /** Expiry policy. */
        private ExpiryPolicy expiryPlc;

        /** Data center Id. */
        private Byte dataCenterId;

        /** Application attributes. */
        private Map<String, String> appAttrs;

        /** Flag indicating whether to handle binary in interceptor. */
        private boolean handleBinary;

        /** */
        Builder() {
            // No context.
        }

        /** */
        Builder(CacheOperationContext ctx) {
            skipStore = ctx.skipStore;
            skipReadThrough = ctx.skipReadThrough;
            noRetries = ctx.noRetries;
            recovery = ctx.recovery;
            readRepairStrategy = ctx.readRepairStrategy;
            keepBinary = ctx.keepBinary;
            expiryPlc = ctx.expiryPlc;
            dataCenterId = ctx.dataCenterId;
            appAttrs = ctx.appAttrs;
            handleBinary = ctx.handleBinaryInInterceptor;
        }

        /**
         * CacheOperationContext with keepBinary flag.
         *
         * @see IgniteInternalCache#keepBinary()
         */
        public Builder keepBinary(boolean keepBinary) {
            this.keepBinary = keepBinary;
            return this;
        }

        /**
         * CacheOperationContext with skipStore flag.
         *
         * @see IgniteInternalCache#withSkipStore()
         */
        public Builder skipStore(boolean skipStore) {
            this.skipStore = skipStore;
            return this;
        }

        /**
         * CacheOperationContext with attributes.
         *
         * @see IgniteInternalCache#withApplicationAttributes(Map)
         */
        public Builder applicationAttributes(Map<String, String> attrs) {
            appAttrs = Collections.unmodifiableMap(new HashMap<>(attrs));
            return this;
        }

        /**
         * CacheOperationContext with skip read through flag.
         *
         * @see IgniteInternalCache#withSkipReadThrough()
         */
        public Builder skipReadThrough(boolean skipReadThrough) {
            this.skipReadThrough = skipReadThrough;
            return this;
        }

        /**
         * CacheOperationContext with handle binary in interceptor execution flag.
         *
         * @see IgniteInternalCache#withHandleBinaryInInterceptor()
         */
        public Builder handleBinaryInInterceptor(boolean handleBinary) {
            this.handleBinary = handleBinary;
            return this;
        }

        /**
         * CacheOperationContext with expiry policy.
         *
         * @see IgniteInternalCache#withExpiryPolicy(ExpiryPolicy)
         */
        public Builder expiryPolicy(ExpiryPolicy expiryPlc) {
            this.expiryPlc = expiryPlc;
            return this;
        }

        /**
         * CacheOperationContext with no retries flag.
         */
        public Builder noRetries(boolean noRetries) {
            this.noRetries = noRetries;
            return this;
        }

        /**
         * CacheOperationContext with Data center id.
         */
        public Builder dataCenterId(Byte dataCenterId) {
            this.dataCenterId = dataCenterId;
            return this;
        }

        /**
         * CacheOperationContext with recovery flag.
         */
        public Builder recovery(boolean recovery) {
            this.recovery = recovery;
            return this;
        }

        /**
         * CacheOperationContext with read repair strategy.
         */
        public Builder readRepairStrategy(ReadRepairStrategy readRepairStrategy) {
            this.readRepairStrategy = readRepairStrategy;
            return this;
        }

        /**
         * Builds cache operations context options.
         *
         * @return Cache operations context options.
         */
        public CacheOperationContext build() {
            return new CacheOperationContext(
                skipStore,
                skipReadThrough,
                keepBinary,
                expiryPlc,
                noRetries,
                dataCenterId,
                recovery,
                readRepairStrategy,
                appAttrs,
                handleBinary);
        }
    }
}
