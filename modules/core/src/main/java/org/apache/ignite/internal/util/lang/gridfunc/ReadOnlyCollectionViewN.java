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

package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.util.GridSerializableCollection;
import org.apache.ignite.internal.util.GridSerializableIterator;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.NotNull;

/**
 * Collections wrapper.
 * A read-only view will be created over the element and given
 * collections and no copying will happen.
 *
 * @param <T> Element type.
 */
public class ReadOnlyCollectionViewN<T> extends GridSerializableCollection<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collections. */
    private final List<Collection<T>> cols = new ArrayList<>();

    /**
     * @param cols Collections.
     */
    public ReadOnlyCollectionViewN(Collection<T>... cols) {
        A.notNull(cols, "cols");
        A.ensure(cols.length > 0, "cols");

        Collections.addAll(this.cols, cols);
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Iterator<T> iterator() {
        return new GridSerializableIterator<T>() {
            private Iterator<Collection<T>> cit = cols.iterator();

            private Iterator<T> it = cit.next().iterator();

            @Override public boolean hasNext() {
                if(it.hasNext())
                    return true;

                while (cit.hasNext()) {
                    it = cit.next().iterator();

                    if(it.hasNext())
                        return true;
                }

                return false;
            }

            @Override public T next() {
                return it.next();
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        for (Collection<T> c : cols) {
            if (c.contains(o))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        int sz = 0;

        for (Collection<T> col : cols)
            sz += col.size();

        return sz;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj instanceof Collection && GridFunc.eqNotOrdered(this, (Collection<?>)obj);
    }
}
