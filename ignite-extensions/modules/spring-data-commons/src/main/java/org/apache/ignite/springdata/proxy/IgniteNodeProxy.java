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

package org.apache.ignite.springdata.proxy;

import java.util.Objects;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

/** Implementation of {@link IgniteProxy} that provides access to Ignite cluster through {@link Ignite} instance. */
public class IgniteNodeProxy implements IgniteProxy {
    /** {@link Ignite} instance to which operations are delegated. */
    protected final Ignite ignite;

    /**
     * @param ignite Ignite instance.
     */
    public IgniteNodeProxy(Ignite ignite) {
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCacheProxy<K, V> getOrCreateCache(String name) {
        return new IgniteNodeCacheProxy<>(ignite.getOrCreateCache(name));
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCacheProxy<K, V> cache(String name) {
        IgniteCache<K, V> cache = ignite.cache(name);

        return cache == null ? null : new IgniteNodeCacheProxy<>(cache);
    }

    /** @return {@link Ignite} instance to which operations are delegated. */
    public Ignite delegate() {
        return ignite;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || getClass() != other.getClass())
            return false;

        return Objects.equals(ignite, ((IgniteNodeProxy)other).ignite);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return ignite.hashCode();
    }
}
