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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ALLOW_ATOMIC_OPS_IN_TX;

/**
 * Cache operation context.
 */
public class CacheOperationContext implements Serializable {
    /** */
    public static final boolean DFLT_ALLOW_ATOMIC_OPS_IN_TX = false;

    /** */
    public static final boolean defaultAllowAtomicOpsInTx() {
        return IgniteSystemProperties.getBoolean(IGNITE_ALLOW_ATOMIC_OPS_IN_TX, DFLT_ALLOW_ATOMIC_OPS_IN_TX);
    }

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

    /** Keep binary flag. */
    private final boolean keepBinary;

    /** Allow atomic cache in transaction. */
    private final boolean allowAtomicOpsInTx;

    /** Expiry policy. */
    private final ExpiryPolicy expiryPlc;

    /** Data center Id. */
    private final Byte dataCenterId;

    /**
     * Constructor with default values.
     */
    public CacheOperationContext() {
        skipStore = false;
        keepBinary = false;
        expiryPlc = null;
        noRetries = false;
        recovery = false;
        readRepairStrategy = null;
        dataCenterId = null;
        allowAtomicOpsInTx = defaultAllowAtomicOpsInTx();
    }

    /**
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @param expiryPlc Expiry policy.
     * @param dataCenterId Data center id.
     * @param readRepairStrategy Read-repair strategy.
     */
    public CacheOperationContext(
        boolean skipStore,
        boolean keepBinary,
        @Nullable ExpiryPolicy expiryPlc,
        boolean noRetries,
        @Nullable Byte dataCenterId,
        boolean recovery,
        @Nullable ReadRepairStrategy readRepairStrategy,
        boolean allowAtomicOpsInTx
    ) {
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;
        this.expiryPlc = expiryPlc;
        this.noRetries = noRetries;
        this.dataCenterId = dataCenterId;
        this.recovery = recovery;
        this.readRepairStrategy = readRepairStrategy;
        this.allowAtomicOpsInTx = allowAtomicOpsInTx;
    }

    /**
     * @return Keep binary flag.
     */
    public boolean isKeepBinary() {
        return keepBinary;
    }

    /**
     * @return {@code True} if data center id is set otherwise {@code false}.
     */
    public boolean hasDataCenterId() {
        return dataCenterId != null;
    }

    /**
     * See {@link IgniteInternalCache#keepBinary()}.
     *
     * @return New instance of CacheOperationContext with keep binary flag.
     */
    public CacheOperationContext keepBinary() {
        return new CacheOperationContext(
            skipStore,
            true,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy,
            allowAtomicOpsInTx);
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
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy,
            allowAtomicOpsInTx);
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
            keepBinary,
            plc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy,
            allowAtomicOpsInTx);
    }

    /**
     * @param noRetries No retries flag.
     * @return Operation context.
     */
    public CacheOperationContext setNoRetries(boolean noRetries) {
        return new CacheOperationContext(
            skipStore,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy,
            allowAtomicOpsInTx);
    }

    /**
     * @param dataCenterId Data center id.
     * @return Operation context.
     */
    public CacheOperationContext setDataCenterId(byte dataCenterId) {
        return new CacheOperationContext(
            skipStore,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy,
            allowAtomicOpsInTx);
    }

    /**
     * @param recovery Recovery flag.
     * @return New instance of CacheOperationContext with recovery flag.
     */
    public CacheOperationContext setRecovery(boolean recovery) {
        return new CacheOperationContext(
            skipStore,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy,
            allowAtomicOpsInTx);
    }

    /**
     * @param readRepairStrategy Read Repair strategy.
     * @return New instance of CacheOperationContext with Read Repair flag.
     */
    public CacheOperationContext setReadRepairStrategy(ReadRepairStrategy readRepairStrategy) {
        return new CacheOperationContext(
            skipStore,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy,
            allowAtomicOpsInTx);
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

    /**
     * @return Operation context.
     */
    public CacheOperationContext setAllowAtomicOpsInTx() {
        return new CacheOperationContext(
            skipStore,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepairStrategy,
            true);
    }

    /**
     * @return Allow in transactions flag.
     */
    public boolean allowedAtomicOpsInTx() {
        return allowAtomicOpsInTx;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheOperationContext.class, this);
    }
}
