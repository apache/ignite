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
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;

import javax.cache.Cache.*;
import java.util.*;

/**
 * Cache-backed iterator.
 */
public class GridCacheIterator<K, V, T> implements GridSerializableIterator<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Base iterator. */
    private final Iterator<? extends Entry<K, V>> it;

    /** Transformer. */
    private final IgniteClosure<Entry<K, V>, T> trans;

    /** Current element. */
    private Entry<K, V> cur;

    /**
     * @param c Cache entry collection.
     * @param trans Transformer.
     * @param filter Filter.
     */
    public GridCacheIterator(Iterable<? extends Entry<K, V>> c,
        IgniteClosure<Entry<K, V>, T> trans,
        IgnitePredicate<Entry<K, V>>[] filter) {
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

        try {
            // Back remove operation by actual cache.
            cur.removex();
        }
        catch (IgniteCheckedException e) {
            throw new GridClosureException(e);
        }
    }
}
