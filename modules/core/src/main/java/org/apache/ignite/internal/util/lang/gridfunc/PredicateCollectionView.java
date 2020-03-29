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
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;

/**
 * Light-weight view on given col with provided predicate.
 *
 * @param <T> Type of the col.
 */
public class PredicateCollectionView<T> extends GridSerializableCollection<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Collection<T> col;

    /** */
    private final IgnitePredicate<? super T>[] preds;

    /**
     * @param col Input col that serves as a base for the view.
     * @param preds Optional preds. If preds are not provided - all elements will be in the view.
     */
    @SafeVarargs
    public PredicateCollectionView(Collection<T> col, IgnitePredicate<? super T>... preds) {
        this.col = col;
        this.preds = preds;
    }

    /** {@inheritDoc} */
    @Override public boolean add(T e) {
        // Pass through (will fail for readonly).
        return GridFunc.isAll(e, preds) && col.add(e);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<T> iterator() {
        return F.iterator0(col, false, preds);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return F.size(col, preds);
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return F.isEmpty(preds) ? col.isEmpty() : !iterator().hasNext();
    }
}
