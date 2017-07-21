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

package org.apache.ignite.internal.util.lang;

import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Convenient adapter for "rich" iterable interface.
 */
public class GridIterableAdapter<T> implements GridIterable<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridIterator<T> impl;

    /**
     * Creates adapter with given iterator implementation.
     *
     * @param impl Iterator implementation.
     */
    public GridIterableAdapter(Iterator<T> impl) {
        A.notNull(impl, "impl");

        this.impl = impl instanceof GridIterator ? (GridIterator<T>)impl : new IteratorWrapper<>(impl);
    }

    /** {@inheritDoc} */
    @Override public GridIterator<T> iterator() {
        return impl;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return impl.hasNext();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        return hasNext();
    }

    /** {@inheritDoc} */
    @Override public T next() {
        return impl.next();
    }

    /** {@inheritDoc} */
    @Override public T nextX() throws IgniteCheckedException {
        return next();
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        impl.remove();
    }

    /** {@inheritDoc} */
    @Override public void removeX() throws IgniteCheckedException {
        remove();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIterableAdapter.class, this);
    }

    /**
     *
     */
    private static class IteratorWrapper<T> extends GridIteratorAdapter<T> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Base iterator. */
        private Iterator<T> it;

        /**
         * Creates wrapper around the iterator.
         *
         * @param it Iterator to wrap.
         */
        IteratorWrapper(Iterator<T> it) {
            this.it = it;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNextX() {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override public T nextX() {
            return it.next();
        }

        /** {@inheritDoc} */
        @Override public void removeX() {
            it.remove();
        }
    }
}