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
import java.util.Iterator;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;

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

    @Override
    public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) {
        assert entries != null;
        CacheWriterException exception = null;
        Iterator<Cache.Entry<? extends K, ? extends V>> iterator = entries.iterator();
        while (iterator.hasNext()){
            try {
                write(iterator.next());
                iterator.remove();
            } catch (CacheWriterException cwe){
                exception = cwe;
            }

        }
        if (entries.size() > 0) throw exception;
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) {
        assert keys != null;

        CacheWriterException exception = null;
        Iterator<?> iterator = keys.iterator();
        while (iterator.hasNext()){
            try {
                delete(iterator.next());
                iterator.remove();
            } catch (CacheWriterException cwe){
                exception = cwe;
            }
        }
        if (keys.size() > 0) throw exception;
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
}