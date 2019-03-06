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