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

package org.apache.ignite.platform;

import java.util.Collection;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * Cache store for .Net tests.
 *
 * @param <K>
 * @param <V>
 */
public class VCacheStore<K, V> implements CacheStore<K, V> {
    /** Ignite instance. */
    @IgniteInstanceResource
    protected Ignite ignite;

    /** */
    private final String typeName;

    /** */
    public VCacheStore(String typeName) {
        this.typeName = typeName;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) throws CacheLoaderException {
        Integer key = 1;

        clo.apply((K)key, (V)ignite.binary().builder(typeName)
            .setField("id", key, Integer.class)
            .setField("name", "v1", String.class)
            .build());
    }

    /** {@inheritDoc} */
    @Override public void sessionEnd(boolean commit) throws CacheWriterException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public V load(K k) throws CacheLoaderException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> loadAll(Iterable<? extends K> iterable) throws CacheLoaderException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> collection) throws CacheWriterException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void delete(Object o) throws CacheWriterException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> collection) throws CacheWriterException {
        throw new UnsupportedOperationException();
    }
}
