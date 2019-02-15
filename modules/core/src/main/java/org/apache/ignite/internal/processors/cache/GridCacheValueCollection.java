/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.cache.Cache;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridSerializableCollection;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Value collection based on provided entries with all remove operations backed
 * by underlying cache.
 */
public class GridCacheValueCollection<K, V> extends GridSerializableCollection<V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache context. */
    private final GridCacheContext<K, V> ctx;

    /** Filter. */
    private final IgnitePredicate<Cache.Entry<K, V>>[] filter;

    /** Base map. */
    private final Map<K, Cache.Entry<K, V>> map;

    /**
     * @param ctx Cache context.
     * @param c Entry collection.
     * @param filter Filter.
     */
    public GridCacheValueCollection(GridCacheContext<K, V> ctx, Collection<? extends Cache.Entry<K, V>> c,
        @Nullable IgnitePredicate<Cache.Entry<K, V>>[] filter) {
        map = new HashMap<>(c.size(), 1.0f);

        assert ctx != null;

        this.ctx = ctx;
        this.filter = filter;

        for (Cache.Entry<K, V> e : c) {
            if (e != null)
                map.put(e.getKey(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<V> iterator() {
        return new GridCacheIterator<K, V, V>(
            ctx,
            map.values(),
            F.<K, V>cacheEntry2Get(),
            ctx.vararg(F0.and(filter, F.<K, V>cacheHasPeekValue()))
        ) {
            {
                advance();
            }

            private V next;

            private void advance() {
                if (next != null)
                    return;

                boolean has;

                while (has = super.hasNext()) {
                    next = super.next();

                    if (next != null)
                        break;
                }

                if (!has)
                    next = null;
            }

            @SuppressWarnings( {"IteratorHasNextCallsIteratorNext"})
            @Override public boolean hasNext() {
                advance();

                return next != null;
            }

            @Override public V next() {
                advance();

                if (next == null)
                    throw new NoSuchElementException();

                V v = next;

                next = null;

                return v;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        A.notNull(o, "o");

        boolean rmv = false;

        for (Iterator<Cache.Entry<K, V>> it = map.values().iterator(); it.hasNext();) {
            Cache.Entry<K, V> e = it.next();

            if (F.isAll(e, filter) && F.eq(o, e.getValue())) {
                it.remove();

                ctx.grid().cache(ctx.name()).remove(e.getKey(), e.getValue());

                rmv = true;
            }
        }

        return rmv;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return F.size(map.values(), filter);
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        A.notNull(o, "o");

        for (Cache.Entry<K, V> e : map.values())
            if (F.isAll(e, filter) && F.eq(e.getValue(), o))
                return true;

        return false;
    }
}