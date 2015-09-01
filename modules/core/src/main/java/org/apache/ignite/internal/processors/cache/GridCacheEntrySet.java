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

package org.apache.ignite.internal.processors.cache;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Entry set backed by cache itself.
 */
public class GridCacheEntrySet<K, V> extends AbstractSet<Cache.Entry<K, V>> {
    /** Cache context. */
    private final GridCacheContext<K, V> ctx;

    /** Filter. */
    private final IgnitePredicate<Cache.Entry<K, V>>[] filter;

    /** Base set. */
    private final Set<Cache.Entry<K, V>> set;

    /**
     * @param ctx Cache context.
     * @param c Entry collection.
     * @param filter Filter.
     */
    public GridCacheEntrySet(GridCacheContext<K, V> ctx, Collection<? extends Cache.Entry<K, V>> c,
        @Nullable IgnitePredicate<Cache.Entry<K, V>>... filter) {
        set = new HashSet<>(c.size(), 1.0f);

        assert ctx != null;

        this.ctx = ctx;
        this.filter = filter;

        for (Cache.Entry<K, V> e : c) {
            if (e != null)
                set.add(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<Cache.Entry<K, V>> iterator() {
        return new GridCacheIterator<>(ctx, set, F.<Cache.Entry<K, V>>identity(), filter);
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        throw new UnsupportedOperationException("clear");
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public boolean remove(Object o) {
        if (!(o instanceof CacheEntryImpl))
            return false;

        Cache.Entry<K, V> e = (Cache.Entry<K,V>)o;

        if (F.isAll(e, filter) && set.remove(e)) {
            try {
                ((IgniteKernal)ctx.grid()).getCache(ctx.name()).remove(e.getKey(), e.getValue());
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteException(ex);
            }

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return F.size(set, filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public boolean contains(Object o) {
        if (!(o instanceof CacheEntryImpl))
            return false;

        Cache.Entry<K,V> e = (Cache.Entry<K, V>)o;

        return F.isAll(e, filter) && set.contains(e);
    }
}