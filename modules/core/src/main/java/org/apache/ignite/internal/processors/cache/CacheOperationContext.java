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
import java.util.UUID;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ALLOW_ATOMIC_OPS_IN_TX;

/**
 * Cache operation context.
 */
public class CacheOperationContext implements Serializable {
    /** */
    //TODO IGNITE-8801 remove this and set default as `false`.
    public static final boolean DFLT_ALLOW_ATOMIC_OPS_IN_TX =
        IgniteSystemProperties.getBoolean(IGNITE_ALLOW_ATOMIC_OPS_IN_TX, true);

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

    /** Read-repair flag. */
    private final boolean readRepair;

    /** Client ID which operates over this projection. */
    private final UUID subjId;

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
        subjId = null;
        keepBinary = false;
        expiryPlc = null;
        noRetries = false;
        recovery = false;
        readRepair = false;
        dataCenterId = null;
        allowAtomicOpsInTx = DFLT_ALLOW_ATOMIC_OPS_IN_TX;
    }

    /**
     * @param skipStore Skip store flag.
     * @param subjId Subject ID.
     * @param keepBinary Keep binary flag.
     * @param expiryPlc Expiry policy.
     * @param dataCenterId Data center id.
     * @param readRepair Read-repair flag.
     */
    public CacheOperationContext(
        boolean skipStore,
        @Nullable UUID subjId,
        boolean keepBinary,
        @Nullable ExpiryPolicy expiryPlc,
        boolean noRetries,
        @Nullable Byte dataCenterId,
        boolean recovery,
        boolean readRepair,
        boolean allowAtomicOpsInTx
    ) {
        this.skipStore = skipStore;
        this.subjId = subjId;
        this.keepBinary = keepBinary;
        this.expiryPlc = expiryPlc;
        this.noRetries = noRetries;
        this.dataCenterId = dataCenterId;
        this.recovery = recovery;
        this.readRepair = readRepair;
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
            subjId,
            true,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepair,
            allowAtomicOpsInTx);
    }

    /**
     * Gets client ID for which this projection was created.
     *
     * @return Client ID.
     */
    @Nullable public UUID subjectId() {
        return subjId;
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
     * See {@link IgniteInternalCache#forSubjectId(UUID)}.
     *
     * @param subjId Subject id.
     * @return New instance of CacheOperationContext with specific subject id.
     */
    public CacheOperationContext forSubjectId(UUID subjId) {
        return new CacheOperationContext(
            skipStore,
            subjId,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepair,
            allowAtomicOpsInTx);
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
            subjId,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepair,
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
            subjId,
            keepBinary,
            plc,
            noRetries,
            dataCenterId,
            recovery,
            readRepair,
            allowAtomicOpsInTx);
    }

    /**
     * @param noRetries No retries flag.
     * @return Operation context.
     */
    public CacheOperationContext setNoRetries(boolean noRetries) {
        return new CacheOperationContext(
            skipStore,
            subjId,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepair,
            allowAtomicOpsInTx);
    }

    /**
     * @param dataCenterId Data center id.
     * @return Operation context.
     */
    public CacheOperationContext setDataCenterId(byte dataCenterId) {
        return new CacheOperationContext(
            skipStore,
            subjId,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepair,
            allowAtomicOpsInTx);
    }

    /**
     * @param recovery Recovery flag.
     * @return New instance of CacheOperationContext with recovery flag.
     */
    public CacheOperationContext setRecovery(boolean recovery) {
        return new CacheOperationContext(
            skipStore,
            subjId,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepair,
            allowAtomicOpsInTx);
    }

    /**
     * @param readRepair Read Repair flag.
     * @return New instance of CacheOperationContext with Read Repair flag.
     */
    public CacheOperationContext setReadRepair(boolean readRepair) {
        return new CacheOperationContext(
            skipStore,
            subjId,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepair,
            allowAtomicOpsInTx);
    }

    /**
     * @return Partition recover flag.
     */
    public boolean recovery() {
        return recovery;
    }

    /**
     * @return Read Repair flag.
     */
    public boolean readRepair() {
        return readRepair;
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
            subjId,
            keepBinary,
            expiryPlc,
            noRetries,
            dataCenterId,
            recovery,
            readRepair,
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
