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

package org.apache.ignite.tests.p2p;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.internal.processors.cache.store.CacheLocalStore;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Test store factory for cache deployment tests.
 */
public class CacheDeploymentTestStoreFactory implements Factory<CacheStore<Integer, String>>, Serializable {
    /** {@inheritDoc} */
    @Override public CacheStore<Integer, String> create() {
        return new TestLocalStore();
    }

    /**
     *
     */
    @CacheLocalStore
    public static class TestLocalStore<K, V> implements CacheStore<K, IgniteBiTuple<V, ?>>, Serializable {
        /** */
        private Map<K, IgniteBiTuple<V, ?>> map = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<K, IgniteBiTuple<V, ?>> clo, @Nullable Object... args)
            throws CacheLoaderException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<V, ?> load(K key) throws CacheLoaderException {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public Map<K, IgniteBiTuple<V, ?>> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
            Map<K, IgniteBiTuple<V, ?>> res = new HashMap<>();

            for (K key : keys) {
                IgniteBiTuple<V, ?> val = map.get(key);

                if (val != null)
                    res.put(key, val);
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends K, ? extends IgniteBiTuple<V, ?>> entry)
            throws CacheWriterException {
            map.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends IgniteBiTuple<V, ?>>> entries)
            throws CacheWriterException {
            for (Cache.Entry<? extends K, ? extends IgniteBiTuple<V, ?>> e : entries)
                map.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            map.remove(key);
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            for (Object key : keys)
                map.remove(key);
        }

        /**
         * Clear store.
         */
        public void clear() {
            map.clear();
        }
    }
}
