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
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

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

    /** Client ID which operates over this projection. */
    private final UUID subjId;

    /** Keep portable flag. */
    private final boolean keepPortable;

    /** Expiry policy. */
    private final ExpiryPolicy expiryPlc;

    /**
     * Constructor with default values.
     */
    public CacheOperationContext() {
        skipStore = false;

        subjId = null;

        keepPortable = false;

        expiryPlc = null;

        noRetries = false;
    }

    /**
     * @param skipStore Skip store flag.
     * @param subjId Subject ID.
     * @param keepPortable Keep portable flag.
     * @param expiryPlc Expiry policy.
     */
    public CacheOperationContext(
        boolean skipStore,
        @Nullable UUID subjId,
        boolean keepPortable,
        @Nullable ExpiryPolicy expiryPlc,
        boolean noRetries) {
        this.skipStore = skipStore;

        this.subjId = subjId;

        this.keepPortable = keepPortable;

        this.expiryPlc = expiryPlc;

        this.noRetries = noRetries;
    }

    /**
     * @return Keep portable flag.
     */
    public boolean isKeepPortable() {
        return keepPortable;
    }

    /**
     * See {@link IgniteInternalCache#keepPortable()}.
     *
     * @return New instance of CacheOperationContext with keep portable flag.
     */
    public CacheOperationContext keepPortable() {
        return new CacheOperationContext(
            skipStore,
            subjId,
            true,
            expiryPlc,
            noRetries);
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
     * See {@link IgniteInternalCache#forSubjectId(UUID)}.
     *
     * @param subjId Subject id.
     * @return New instance of CacheOperationContext with specific subject id.
     */
    public CacheOperationContext forSubjectId(UUID subjId) {
        return new CacheOperationContext(
            skipStore,
            subjId,
            keepPortable,
            expiryPlc,
            noRetries);
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
            keepPortable,
            expiryPlc,
            noRetries);
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
            true,
            plc,
            noRetries);
    }

    /**
     * @param noRetries No retries flag.
     * @return Operation context.
     */
    public CacheOperationContext setNoRetries(boolean noRetries) {
        return new CacheOperationContext(
            skipStore,
            subjId,
            keepPortable,
            expiryPlc,
            noRetries
        );
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