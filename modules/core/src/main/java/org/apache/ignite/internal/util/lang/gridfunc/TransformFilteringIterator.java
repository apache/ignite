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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Iterator from given iter and optional filtering predicate.
 */
public class TransformFilteringIterator<T2, T1> extends GridIteratorAdapter<T2> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteClosure<? super T1, T2> clos;

    /** */
    private final boolean readOnly;

    /** */
    private final IgnitePredicate<? super T1>[] preds;

    /** */
    private T1 elem;

    /** */
    private boolean more;

    /** */
    private boolean moved;

    /** */
    private Iterator<? extends T1> iter;

    /**
     * @param iter Input iter.
     * @param clos Transforming closure to convert from T1 to T2.
     * @param readOnly If {@code true}, then resulting iter will not allow modifications to the underlying
     * collection.
     * @param preds Optional filtering predicates.
     */
    public TransformFilteringIterator(Iterator<? extends T1> iter, IgniteClosure<? super T1, T2> clos,
        boolean readOnly,
        IgnitePredicate<? super T1>... preds) {
        this.clos = clos;
        this.readOnly = readOnly;
        this.preds = preds;
        this.iter = iter;
        this.moved = true;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() {
        if (GridFunc.isEmpty(preds))
            return iter.hasNext();
        else {
            if (!moved)
                return more;
            else {
                more = false;

                while (iter.hasNext()) {
                    elem = iter.next();

                    boolean isAll = true;

                    for (IgnitePredicate<? super T1> r : preds)
                        if (r != null && !r.apply(elem)) {
                            isAll = false;

                            break;
                        }

                    if (isAll) {
                        more = true;
                        moved = false;

                        return true;
                    }
                }

                elem = null; // Give to GC.

                return false;
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public T2 nextX() {
        if (GridFunc.isEmpty(preds))
            return clos.apply(iter.next());
        else {
            if (hasNext()) {
                moved = true;

                return clos.apply(elem);
            }
            else
                throw new NoSuchElementException();
        }
    }

    /** {@inheritDoc} */
    @Override public void removeX() {
        if (readOnly)
            throw new UnsupportedOperationException("Cannot modify read-only iter.");

        iter.remove();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransformFilteringIterator.class, this);
    }
}
