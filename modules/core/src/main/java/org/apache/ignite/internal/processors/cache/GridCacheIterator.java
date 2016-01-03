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

import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.internal.util.GridSerializableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Cache-backed iterator.
 */
public class GridCacheIterator<K, V, T> implements GridSerializableIterator<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Base iterator. */
    private final Iterator<? extends Cache.Entry<K, V>> it;

    /** Transformer. */
    private final IgniteClosure<Cache.Entry<K, V>, T> trans;

    /** Current element. */
    private Cache.Entry<K, V> cur;

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /**
     * @param cctx Context.
     * @param c Cache entry collection.
     * @param trans Transformer.
     * @param filter Filter.
     */
    public GridCacheIterator(
        GridCacheContext<K, V> cctx,
        Iterable<? extends Cache.Entry<K, V>> c,
        IgniteClosure<Cache.Entry<K, V>, T> trans,
        IgnitePredicate<Cache.Entry<K, V>>[] filter
    ) {
        this.cctx = cctx;

        it = F.iterator0(c, false, filter);

        this.trans = trans;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        if (!it.hasNext()) {
            cur = null;

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public T next() {
        return trans.apply(cur = it.next());
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        it.remove();

        cctx.grid().cache(cctx.name()).remove(cur.getKey(), cur.getValue());
    }
}