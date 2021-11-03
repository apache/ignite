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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import org.jetbrains.annotations.NotNull;

public class FilteringIterator<T> implements Iterator<T> {
    private final Iterator<T> delegate;

    private final Predicate<T> pred;

    private T cur;

    public FilteringIterator(
            @NotNull Iterator<T> delegate,
            @NotNull Predicate<T> pred
    ) {
        this.delegate = delegate;
        this.pred = pred;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        advance();

        return cur != null;
    }

    /** {@inheritDoc} */
    @Override
    public T next() {
        advance();

        if (cur == null) {
            throw new NoSuchElementException();
        }

        T tmp = cur;

        cur = null;

        return tmp;
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
        delegate.remove();
    }

    /**
     *
     */
    private void advance() {
        if (cur != null) {
            return;
        }

        while (delegate.hasNext() && cur == null) {
            cur = delegate.next();

            if (!pred.test(cur)) {
                cur = null;
            }
        }
    }
}
