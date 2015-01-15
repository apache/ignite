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

package org.gridgain.grid.cache.store;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cache storage convenience adapter. It provides default implementation for bulk operations, such
 * as {@link #loadAll(IgniteTx, Collection, org.apache.ignite.lang.IgniteBiInClosure)},
 * {@link #putAll(IgniteTx, Map)}, and {@link #removeAll(IgniteTx, Collection)}
 * by sequentially calling corresponding {@link #load(IgniteTx, Object)},
 * {@link #put(IgniteTx, Object, Object)}, and {@link #remove(IgniteTx, Object)}
 * operations. Use this adapter whenever such behaviour is acceptable. However in many cases
 * it maybe more preferable to take advantage of database batch update functionality, and therefore
 * default adapter implementation may not be the best option.
 * <p>
 * Note that method {@link #loadCache(org.apache.ignite.lang.IgniteBiInClosure, Object...)} has empty
 * implementation because it is essentially up to the user to invoke it with
 * specific arguments.
 */
public abstract class GridCacheStoreAdapter<K, V> implements GridCacheStore<K, V> {
    /**
     * Default empty implementation. This method needs to be overridden only if
     * {@link GridCache#loadCache(org.apache.ignite.lang.IgniteBiPredicate, long, Object...)} method
     * is explicitly called.
     *
     * @param clo {@inheritDoc}
     * @param args {@inheritDoc}
     * @throws IgniteCheckedException {@inheritDoc}
     */
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, Object... args)
        throws IgniteCheckedException {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public void loadAll(@Nullable IgniteTx tx, Collection<? extends K> keys,
        IgniteBiInClosure<K, V> c) throws IgniteCheckedException {
        assert keys != null;

        for (K key : keys) {
            V v = load(tx, key);

            if (v != null)
                c.apply(key, v);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(IgniteTx tx, Map<? extends K, ? extends V> map)
        throws IgniteCheckedException {
        assert map != null;

        for (Map.Entry<? extends K, ? extends V> e : map.entrySet())
            put(tx, e.getKey(), e.getValue());
    }

    /** {@inheritDoc} */
    @Override public void removeAll(IgniteTx tx, Collection<? extends K> keys)
        throws IgniteCheckedException {
        assert keys != null;

        for (K key : keys)
            remove(tx, key);
    }

    /**
     * Default empty implementation for ending transactions. Note that if explicit cache
     * transactions are not used, then transactions do not have to be explicitly ended -
     * for all other cases this method should be overridden with custom commit/rollback logic.
     *
     * @param tx {@inheritDoc}
     * @param commit {@inheritDoc}
     * @throws IgniteCheckedException {@inheritDoc}
     */
    @Override public void txEnd(IgniteTx tx, boolean commit) throws IgniteCheckedException {
        // No-op.
    }
}
