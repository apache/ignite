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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;

/**
 * Light-weight view on given collection with provided predicate.
 *
 * @param <T1> Element type after transformation.
 * @param <T2> Element type.
 */
public class TransformCollectionView<T1, T2> extends GridSerializableCollection<T1> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Collection<? extends T2> col;

    /** */
    private final IgniteClosure<? super T2, T1> clos;

    /** */
    private final IgnitePredicate<? super T2>[] preds;

    /**
     * @param col Input collection that serves as a base for the view.
     * @param clos Transformation closure.
     * @param preds Optional predicated. If predicates are not provided - all elements will be in the view.
     */
    @SafeVarargs
    public TransformCollectionView(Collection<? extends T2> col,
        IgniteClosure<? super T2, T1> clos, IgnitePredicate<? super T2>... preds) {
        this.col = col;
        this.clos = clos;
        this.preds = preds;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<T1> iterator() {
        return F.<T2, T1>iterator(col, clos, true, preds);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return F.isEmpty(preds) ? col.size() : F.size(iterator());
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return F.isEmpty(preds) ? col.isEmpty() : !iterator().hasNext();
    }
}
