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

import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.internal.util.GridSerializableCollection;
import org.apache.ignite.internal.util.GridSerializableIterator;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.jetbrains.annotations.NotNull;

/**
 * Collections wrapper.
 * A read-only view will be created over the element and given
 * collections and no copying will happen.
 *
 * @param <T> Element type.
 */
public class ReadOnlyCollectionView2X<T> extends GridSerializableCollection<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** First collection. */
    private final Collection<? extends T> c1;

    /** SecondCollection. */
    private final Collection<? extends T> c2;

    /**
     * @param c1 First collection.
     * @param c2 SecondCollection.
     */
    public ReadOnlyCollectionView2X(Collection<? extends T> c1, Collection<? extends T> c2) {
        this.c1 = c1;
        this.c2 = c2;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Iterator<T> iterator() {
        return new GridSerializableIterator<T>() {
            private Iterator<? extends T> it1 = c1.iterator();
            private Iterator<? extends T> it2 = c2.iterator();

            @Override public boolean hasNext() {
                if (it1 != null)
                    if (!it1.hasNext())
                        it1 = null;
                    else
                        return true;

                return it2.hasNext();
            }

            @Override public T next() {
                return it1 != null ? it1.next() : it2.next();
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        return c1.contains(o) || c2.contains(o);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return c1.size() + c2.size();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj instanceof Collection && GridFunc.eqNotOrdered(this, (Collection<?>)obj);
    }
}
