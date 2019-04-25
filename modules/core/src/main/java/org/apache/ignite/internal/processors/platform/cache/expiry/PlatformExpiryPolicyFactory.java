/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform.cache.expiry;

import javax.cache.configuration.Factory;

/**
 * Platform expiry policy factory.
 */
public class PlatformExpiryPolicyFactory implements Factory<PlatformExpiryPolicy> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final long create;

    /** */
    private final long update;

    /** */
    private final long access;

    /**
     * Ctor.
     *
     * @param create Expiry for create.
     * @param update Expiry for update.
     * @param access Expiry for access.
     */
    public PlatformExpiryPolicyFactory(long create, long update, long access) {
        this.create = create;
        this.update = update;
        this.access = access;
    }

    /**
     * @return Create expiry.
     */
    public long getCreate() {
        return create;
    }

    /**
     * @return Update expiry.
     */
    public long getUpdate() {
        return update;
    }

    /**
     * @return Access expiry.
     */
    public long getAccess() {
        return access;
    }

    /** {@inheritDoc} */
    @Override public PlatformExpiryPolicy create() {
        return new PlatformExpiryPolicy(create, update, access);
    }
}