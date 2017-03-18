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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;

/**
 * Light-weight view on given collection with provided predicate.
 *
 * @param <T> Type of the collection.
 */
public class GridSerializableCollectionPredicateWrapper<T> extends GridSerializableCollection<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Collection<T> collection;

    /** */
    private final IgnitePredicate<? super T>[] predicates;

    /**
     * @param collection Input collection that serves as a base for the view.
     * @param predicates Optional predicates. If predicates are not provided - all elements will be in the view.
     */
    public GridSerializableCollectionPredicateWrapper(Collection<T> collection, IgnitePredicate<? super T>... predicates) {
        this.collection = collection;
        this.predicates = predicates;
    }

    /** {@inheritDoc} */
    @Override public boolean add(T e) {
        // Pass through (will fail for readonly).
        return GridFunc.isAll(e, predicates) && collection.add(e);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<T> iterator() {
        return F.iterator0(collection, false, predicates);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return F.size(collection, predicates);
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return F.isEmpty(predicates) ? collection.isEmpty() : !iterator().hasNext();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSerializableCollectionPredicateWrapper.class, this);
    }
}
