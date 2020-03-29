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
import org.jetbrains.annotations.Nullable;

/**
 * Collection wrapper.
 * A read-only view will be created over the element and given
 * col and no copying will happen.
 *
 * @param <T> Element type.
 */
public class ReadOnlyCollectionView<T> extends GridSerializableCollection<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collection. */
    private final Collection<T> col;

    /** First element in the col. */
    private final T elem;

    /**
     * @param col Collection to wrap.
     * @param elem First element.
     */
    public ReadOnlyCollectionView(@NotNull Collection<T> col, @NotNull T elem) {
        this.col = col;
        this.elem = elem;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Iterator<T> iterator() {
        return new GridSerializableIterator<T>() {
            private Iterator<T> it;

            @Override public boolean hasNext() {
                return it == null || it.hasNext();
            }

            @Nullable @Override public T next() {
                if (it == null) {
                    it = col.iterator();

                    return elem;
                }

                return it.next();
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return col.size() + 1;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj instanceof Collection && GridFunc.eqNotOrdered(this, (Collection)obj);
    }
}
