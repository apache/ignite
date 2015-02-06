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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import javax.cache.Cache.*;
import java.util.*;

/**
 * Key set based on provided entries with all remove operations backed
 * by underlying cache.
 */
public class GridCacheKeySet<K, V> extends GridSerializableSet<K> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache context. */
    private final GridCacheContext<K, V> ctx;

    /** Filter. */
    private final IgnitePredicate<Entry<K, V>>[] filter;

    /** Base map. */
    private final Map<K, Entry<K, V>> map;

    /**
     * @param ctx Cache context.
     * @param c Entry collection.
     * @param filter Filter.
     */
    public GridCacheKeySet(GridCacheContext<K, V> ctx, Collection<? extends Entry<K, V>> c,
        IgnitePredicate<Entry<K, V>>[] filter) {
        map = new HashMap<>(c.size(), 1.0f);

        assert ctx != null;

        this.ctx = ctx;
        this.filter = filter == null ? CU.<K, V>empty() : filter;

        for (Entry<K, V> e : c) {
            if (e != null)
                map.put(e.getKey(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public Iterator<K> iterator() {
        return new GridCacheIterator<>(map.values(), F.<K, V>cacheEntry2Key(), filter);
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        ctx.cache().clearLocally0(F.viewReadOnly(map.values(), F.<K>mapEntry2Key(), filter), CU.<K, V>empty());

        map.clear();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SuspiciousMethodCalls"})
    @Override public boolean remove(Object o) {
        Entry<K, V> e = map.get(o);

        if (e == null || !F.isAll(e, filter))
            return false;

        map.remove(o);

        //                // TODO ignite-96

//        try {
//            e.removex();
//        }
//        catch (IgniteCheckedException ex) {
//            throw new IgniteException(ex);
//        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return F.size(map.values(), filter);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SuspiciousMethodCalls"})
    @Override public boolean contains(Object o) {
        Entry<K, V> e = map.get(o);

        return e != null && F.isAll(e, filter);
    }
}
