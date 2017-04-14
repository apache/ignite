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

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Concatenates multiple iterators as single one.
 *
 * @param <T> Elements type.
 */
public class MultipleIterator<T> implements Iterator<T>, Serializable {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    private final Iterator<Iterator<T>> iters;

    /** */
    private Iterator<T> it;

    /** */
    private Iterator<T> last;

    /** */
    private T next;

    /**
     * @param iters Iterator over iterators.
     */
    public MultipleIterator(Iterator<Iterator<T>> iters) {
        this.iters = iters;
        it = iters.next();
        advance();
    }

    /** */
    private void advance() {
        for (; ; ) {
            if (it.hasNext()) {
                next = it.next();

                assert next != null;

                return;
            }

            if (!iters.hasNext())
                return;

            it = iters.next();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return next != null;
    }

    /** {@inheritDoc} */
    @Override public T next() {
        T res = next;

        if (res == null)
            throw new NoSuchElementException();

        next = null;

        last = it;

        advance();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        if (last == null)
            throw new IllegalStateException();

        last.remove();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MultipleIterator.class, this);
    }
}
