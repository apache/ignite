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

package org.apache.ignite.cache.store;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * Cache storage convenience adapter. It provides default implementation for bulk operations, such
 * as {@link #loadAll(Iterable)},
 * {@link #writeAll(Collection)}, and {@link #deleteAll(Collection)}
 * by sequentially calling corresponding {@link #load(Object)},
 * {@link #write(Cache.Entry)}, and {@link #delete(Object)}
 * operations. Use this adapter whenever such behaviour is acceptable. However in many cases
 * it maybe more preferable to take advantage of database batch update functionality, and therefore
 * default adapter implementation may not be the best option.
 * <p>
 * Note that method {@link #loadCache(IgniteBiInClosure, Object...)} has empty
 * implementation because it is essentially up to the user to invoke it with
 * specific arguments.
 */
public abstract class CacheStoreAdapter<K, V> implements CacheStore<K, V> {
    /**
     * Default empty implementation. This method needs to be overridden only if
     * {@link org.apache.ignite.IgniteCache#loadCache(IgniteBiPredicate, Object...)} method
     * is explicitly called.
     *
     * @param clo {@inheritDoc}
     * @param args {@inheritDoc}
     */
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, Object... args) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> loadAll(Iterable<? extends K> keys) {
        assert keys != null;

        Map<K, V> loaded = new HashMap<>();

        for (K key : keys) {
            V v = load(key);

            if (v != null)
                loaded.put(key, v);
        }

        return loaded;
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) {
        assert entries != null;

        for (Cache.Entry<? extends K, ? extends V> e : entries)
            write(e);
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) {
        assert keys != null;

        for (Object key : keys)
            delete(key);
    }

    /**
     * Default empty implementation for ending transactions. Note that if explicit cache
     * transactions are not used, then transactions do not have to be explicitly ended -
     * for all other cases this method should be overridden with custom commit/rollback logic.
     *
     * @param commit {@inheritDoc}
     */
    @Override public void sessionEnd(boolean commit) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheStoreAdapter.class, this);
    }
}