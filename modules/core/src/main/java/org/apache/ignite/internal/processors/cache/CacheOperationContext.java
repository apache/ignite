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
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.CacheReturnMode.DESERIALIZED;

/**
 * Cache operation context.
 */
public class CacheOperationContext implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Skip store. */
    @GridToStringInclude
    private final boolean skipStore;

    /** No retries flag. */
    @GridToStringInclude
    private final boolean noRetries;

    /** */
    private final boolean recovery;

    /** Read-repair strategy. */
    private final ReadRepairStrategy readRepairStrategy;

    /** Cache return mode. */
    private final CacheReturnMode cacheReturnMode;

    /** Expiry policy. */
    private final ExpiryPolicy expiryPlc;

    /** Data center Id. */
    private final Byte dataCenterId;

    /**
     * Constructor with default values.
     */
    public CacheOperationContext() {
        skipStore = false;
        cacheReturnMode = DESERIALIZED;
        expiryPlc = null;
        noRetries = false;
        recovery = false;
        readRepairStrategy = null;
        dataCenterId = null;
    }

    /**
     * @param skipStore Skip store flag.
     * @param cacheReturnMode Cache return mode.
     * @param expiryPlc Expiry policy.
     * @param dataCenterId Data center id.
     * @param readRepairStrategy Read-repair strategy.
     */
    public CacheOperationContext(
        boolean skipStore,
        CacheReturnMode cacheReturnMode,
        @Nullable ExpiryPolicy expiryPlc,
        boolean noRetries,
        @Nullable Byte dataCenterId,
        boolean recovery,
        @Nullable ReadRepairStrategy readRepairStrategy
    ) {
        this.skipStore = skipStore;
        this.cacheReturnMode = cacheReturnMode;
        this.expiryPlc = expiryPlc;
        this.noRetries = noRetries;
        this.dataCenterId = dataCenterId;
        this.recovery = recovery;
        this.readRepairStrategy = readRepairStrategy;
    }

    /** */
    public CacheReturnMode cacheReturnMode() {
        return cacheReturnMode;
    }

    /**
     * @return {@code True} if data center id is set otherwise {@code false}.
     */
    public boolean hasDataCenterId() {
        return dataCenterId != null;
    }

    /**
     * Gets data center ID.
     *
     * @return Datacenter ID.
     */
    @Nullable public Byte dataCenterId() {
        return dataCenterId;
    }

    /**
     * @return Skip store.
     */
    public boolean skipStore() {
        return skipStore;
    }

    /**
     * See {@link IgniteInternalCache#setSkipStore(boolean)}.
     *
     * @param skipStore Skip store flag.
     * @return New instance of CacheOperationContext with skip store flag.
     */
    public CacheOperationContext setSkipStore(boolean skipStore) {
        return new CacheOperationContext(
            skipStore,
            cacheReturnMode,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy);
    }

    /**
     * @return {@link ExpiryPolicy} associated with this projection.
     */
    @Nullable public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /**
     * See {@link IgniteInternalCache#withExpiryPolicy(ExpiryPolicy)}.
     *
     * @param plc {@link ExpiryPolicy} to associate with this projection.
     * @return New instance of CacheOperationContext with skip store flag.
     */
    public CacheOperationContext withExpiryPolicy(ExpiryPolicy plc) {
        return new CacheOperationContext(
            skipStore,
            cacheReturnMode,
            plc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy);
    }

    /** */
    public CacheOperationContext withCacheReturnMode(CacheReturnMode cacheReturnMode) {
        return new CacheOperationContext(
            skipStore,
            cacheReturnMode,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy);
    }

    /**
     * @param noRetries No retries flag.
     * @return Operation context.
     */
    public CacheOperationContext setNoRetries(boolean noRetries) {
        return new CacheOperationContext(
            skipStore,
            cacheReturnMode,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy);
    }

    /**
     * @param dataCenterId Data center id.
     * @return Operation context.
     */
    public CacheOperationContext setDataCenterId(byte dataCenterId) {
        return new CacheOperationContext(
            skipStore,
            cacheReturnMode,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy);
    }

    /**
     * @param recovery Recovery flag.
     * @return New instance of CacheOperationContext with recovery flag.
     */
    public CacheOperationContext setRecovery(boolean recovery) {
        return new CacheOperationContext(
            skipStore,
            cacheReturnMode,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy);
    }

    /**
     * @param readRepairStrategy Read Repair strategy.
     * @return New instance of CacheOperationContext with Read Repair flag.
     */
    public CacheOperationContext setReadRepairStrategy(ReadRepairStrategy readRepairStrategy) {
        return new CacheOperationContext(
            skipStore,
            cacheReturnMode,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy);
    }

    /**
     * @return Partition recover flag.
     */
    public boolean recovery() {
        return recovery;
    }

    /**
     * @return Read Repair strategy.
     */
    public ReadRepairStrategy readRepairStrategy() {
        return readRepairStrategy;
    }

    /**
     * @return No retries flag.
     */
    public boolean noRetries() {
        return noRetries;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheOperationContext.class, this);
    }
}
