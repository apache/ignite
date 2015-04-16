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

import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import java.io.*;
import java.util.*;

/**
 * Cache projection context.
 */
public class CacheProjectionContext implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Skip store. */
    @GridToStringInclude
    private final boolean skipStore;

    /** Client ID which operates over this projection, if any, */
    private final UUID subjId;

    /** */
    private final boolean keepPortable;

    /** */
    private final ExpiryPolicy expiryPlc;

    /**
     * @param skipStore Skip store flag.
     * @param subjId Subject ID.
     * @param keepPortable Keep portable flag.
     * @param expiryPlc Expiry policy.
     */
    public CacheProjectionContext(
        boolean skipStore,
        @Nullable UUID subjId,
        boolean keepPortable,
        @Nullable ExpiryPolicy expiryPlc) {
        this.skipStore = skipStore;

        this.subjId = subjId;

        this.keepPortable = keepPortable;

        this.expiryPlc = expiryPlc;
    }

    /**
     * @return Keep portable flag.
     */
    public boolean isKeepPortable() {
        return keepPortable;
    }

    /**
     * @return {@code True} if portables should be deserialized.
     */
    public boolean deserializePortables() {
        return !keepPortable;
    }

    /**
     * See {@link InternalCache#keepPortable()}.
     *
     * @return New instance of CacheProjectionContext with keep portable flag.
     */
    public CacheProjectionContext keepPortable() {
        return new CacheProjectionContext(
            skipStore,
            subjId,
            true,
            expiryPlc);
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
     * See {@link InternalCache#forSubjectId(UUID)}.
     *
     * @return New instance of CacheProjectionContext with specific subject id.
     */
    public CacheProjectionContext forSubjectId(UUID subjId) {
        return new CacheProjectionContext(
            skipStore,
            subjId,
            keepPortable,
            expiryPlc);
    }

    /**
     * @return Skip store.
     */
    public boolean skipStore() {
        return skipStore;
    }

    /**
     * See {@link InternalCache#setSkipStore(boolean)}.
     *
     * @return New instance of CacheProjectionContext with skip store flag.
     */
    public CacheProjectionContext setSkipStore(boolean skipStore) {
        return new CacheProjectionContext(
            skipStore,
            subjId,
            keepPortable,
            expiryPlc);
    }

    /**
     * @return {@link ExpiryPolicy} associated with this projection.
     */
    @Nullable public ExpiryPolicy expiry() {
        return expiryPlc;
    }

    /**
     * See {@link InternalCache#withExpiryPolicy(ExpiryPolicy)}.
     *
     * @param plc {@link ExpiryPolicy} to associate with this projection.
     * @return New instance of CacheProjectionContext with skip store flag.
     */
    public CacheProjectionContext withExpiryPolicy(ExpiryPolicy plc) {
        return new CacheProjectionContext(
            skipStore,
            subjId,
            true,
            plc);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheProjectionContext.class, this);
    }
}
