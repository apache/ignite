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

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for cache expiry policy configuration properties.
 */
public class VisorCacheExpiryPolicyConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** After create expire policy. */
    private String create;

    /** After access expire policy. */
    private String access;

    /** After update expire policy. */
    private String update;

    /**
     * Default constructor.
     */
    public VisorCacheExpiryPolicyConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for cache expiry policy configuration.
     *
     * @param ccfg Cache configuration.
     */
    public VisorCacheExpiryPolicyConfiguration(CacheConfiguration ccfg) {
        final Factory<ExpiryPolicy> expiryPolicyFactory = ccfg.getExpiryPolicyFactory();
        ExpiryPolicy expiryPolicy = expiryPolicyFactory.create();

        create = durationToString(expiryPolicy.getExpiryForCreation());
        access = durationToString(expiryPolicy.getExpiryForAccess());
        update = durationToString(expiryPolicy.getExpiryForUpdate());
    }

    /**
     * @return After create expire policy.
     */
    public String getCreate() {
        return create;
    }

    /**
     * @return After access expire policy.
     */
    public String getAccess() {
        return access;
    }

    /**
     * @return After update expire policy.
     */
    public String getUpdate() {
        return update;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, create);
        U.writeString(out, access);
        U.writeString(out, update);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        create = U.readString(in);
        access = U.readString(in);
        update = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheExpiryPolicyConfiguration.class, this);
    }

    /**
     * Convert Duration to String
     * @param duration Duration
     * @return Duration class string representation
     */
    private String durationToString(Duration duration) {
        return duration != null ? String.valueOf(duration.getDurationAmount()) + " " + duration.getTimeUnit() : null;
    }
}
