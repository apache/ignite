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

package org.apache.ignite.configuration;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Platform cache configuration.
 * <p>
 * Additional caching mechanism on platform side (.NET).
 */
public class PlatformCacheConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key type name. */
    private String keyTypeName;

    /** Value type name. */
    private String valueTypeName;

    /** Whether to cache binary objects. */
    private boolean keepBinary;

    /**
     * Gets fully-qualified platform type name of the cache key used for the local map.
     * When not set, object-based map is used, which can reduce performance and increase allocations due to boxing.
     *
     * @return Key type name.
     */
    public String getKeyTypeName() {
        return keyTypeName;
    }

    /**
     * Sets fully-qualified platform type name of the cache key used for the local map.
     * When not set, object-based map is used, which can reduce performance and increase allocations due to boxing.
     *
     * @param keyTypeName Key type name.
     * @return {@code this} for chaining.
     */
    public PlatformCacheConfiguration setKeyTypeName(String keyTypeName) {
        this.keyTypeName = keyTypeName;

        return this;
    }

    /**
     * Gets fully-qualified platform type name of the cache value used for the local map.
     * When not set, object-based map is used, which can reduce performance and increase allocations due to boxing.
     *
     * @return Key type name.
     */
    public String getValueTypeName() {
        return valueTypeName;
    }

    /**
     * Sets fully-qualified platform type name of the cache value used for the local map.
     * When not set, object-based map is used, which can reduce performance and increase allocations due to boxing.
     *
     * @param valueTypeName Key type name.
     * @return {@code this} for chaining.
     */
    public PlatformCacheConfiguration setValueTypeName(String valueTypeName) {
        this.valueTypeName = valueTypeName;

        return this;
    }

    /**
     * Gets a value indicating whether platform cache should store keys and values in binary form.
     *
     * @return Whether binary mode is enabled.
     */
    public boolean isKeepBinary() {
        return keepBinary;
    }

    /**
     * Sets a value indicating whether platform cache should store keys and values in binary form.
     *
     * @param keepBinary Whether binary mode is enabled.
     * @return {@code this} for chaining.
     */
    public PlatformCacheConfiguration setKeepBinary(boolean keepBinary) {
        this.keepBinary = keepBinary;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformCacheConfiguration.class, this, super.toString());
    }
}
