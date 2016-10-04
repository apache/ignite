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

package org.apache.ignite.binary;

import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.internal.binary.FieldsListHashCodeResolver;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Mode of generating hash codes for keys created with {@link BinaryObjectBuilder}.
 */
public enum BinaryKeyHashingMode {
    /**
     * Default (also legacy pre 1.8) mode. Use this mode if you use no SQL DML commands - INSERT, UPDATE, DELETE, MERGE,
     * in other words, if you put data to cache NOT via SQL.
     * Effect from choosing this mode is identical to omitting mode settings from key configuration at all.
     */
    DEFAULT,

    /**
     * Generate hash code based upon serialized representation of binary object fields - namely, byte array constructed
     * by {@link BinaryObjectBuilder}. Use this mode if you are NOT planning to retrieve data from cache via
     * ordinary cache methods like {@link IgniteCache#get(Object)}, {@link IgniteCache#getAll(Set)}, etc. -
     * it's an convenient way to manipulate and retrieve binary data in cache only via full-scale SQL features
     * with as little additional configuration overhead as choosing this mode.
     */
    BYTES_HASH,

    /**
     * Generate hash code based upon on list of fields declared in {@link BinaryObjectBuilder}
     * (not in {@link BinaryObject} as hash code has to be computed <b>before</b> {@link BinaryObject} is fully built) -
     * this mode requires that you set {@link CacheKeyConfiguration#binHashCodeFields} for it to work.
     */
    FIELDS_HASH {
        /** {@inheritDoc} */
        @Override public BinaryObjectHashCodeResolver createResolver(CacheKeyConfiguration keyConfiguration) {
            return new FieldsListHashCodeResolver(keyConfiguration.getBinaryHashCodeFields());
        }
    },

    /**
     * Generate hash code arbitrarily based on {@link BinaryObjectBuilder} using specified class implementing
     * {@link BinaryObjectHashCodeResolver}- this mode requires that you set
     * {@link CacheKeyConfiguration#binHashCodeRslvrClsName} for it to work.
     */
    CUSTOM {
        /** {@inheritDoc} */
        @Override public BinaryObjectHashCodeResolver createResolver(CacheKeyConfiguration keyConfiguration) {
            String clsName = keyConfiguration.getTypeName();

            String rslvrClsName = keyConfiguration.getBinaryHashCodeResolverClassName();

            if (F.isEmpty(rslvrClsName))
                throw new IllegalArgumentException("No custom hash code resolver class name specified, please review " +
                    "your configuration [clsName=" + clsName + ']');

            Class<?> rslvrCls = U.classForName(rslvrClsName, null);

            if (rslvrCls == null)
                throw new IllegalArgumentException("Hash code resolver class not found [clsName=" + clsName +
                    ", rslvrClsName=" + rslvrClsName + ']');

            if (!BinaryObjectHashCodeResolver.class.isAssignableFrom(rslvrCls))
                throw new IllegalArgumentException("Hash code resolver class does not implement " +
                    "BinaryObjectHashCodeResolver interface [clsName=" + clsName + ", rslvrClsName=" +
                    rslvrClsName + ']');

            try {
                return (BinaryObjectHashCodeResolver) U.newInstance(rslvrCls);
            }
            catch (IgniteCheckedException e) {
                throw new BinaryObjectException("Failed to instantiate hash code resolver [clsName=" +
                    clsName + ", rslvrClsName=" + rslvrClsName + ']', e);
            }
        }
    };

    /**
     * @return {@code true} if this mode relies on params set in {@link CacheKeyConfiguration}.
     */
    public final boolean isConfigurable() {
        switch (this) {
            case FIELDS_HASH:
            case CUSTOM:
                return true;
            case DEFAULT:
            case BYTES_HASH:
                return false;
            default:
                throw new IllegalArgumentException("Unexpected key hashing mode [mode=" + this.name() + ']');
        }
    }

    /**
     * Instantiate {@link BinaryObjectHashCodeResolver} based on given {@link CacheKeyConfiguration}.
     *
     * @param keyConfiguration Configuration to create resolver based upon.
     * @return Hash code resolver.
     */
    public BinaryObjectHashCodeResolver createResolver(CacheKeyConfiguration keyConfiguration) {
        assert !isConfigurable() : "Configurable hashing mode does not override createResolver";

        throw new UnsupportedOperationException("This binary keys hashing mode is not configurable, " +
            "only modes " + FIELDS_HASH.name() + " and " + CUSTOM.name() + " are.");
    }
}
