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
package org.apache.ignite.internal.util;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Provides read-only collection interface to objects subarray.
 */
public class GridReadOnlyArrayView<T> extends AbstractCollection<T> {
    /** Array. */
    private final T[] arr;

    /** Array index view starts from (inclusive). */
    private final int from;

    /** Array index view ends with (exclusive). */
    private final int to;

    /**
     * @param arr Array.
     * @param from Array index view starts from (inclusive).
     * @param to Array index view ends with (exclusive).
     */
    public GridReadOnlyArrayView(T[] arr, int from, int to) {
        A.ensure(from <= to, "Lower bound is greater than upper bound [from=" + from + ", to=" + to + ']');

        this.arr = arr;
        this.from = from;
        this.to = to;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return to - from;
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        return new Itr();
    }

    /**
     * Iterator.
     */
    private class Itr implements Iterator<T> {
        /** Cursor index. */
        int cursor = from;

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return cursor < to;
        }

        /** {@inheritDoc} */
        @Override public T next() {
            if (cursor >= to)
                throw new NoSuchElementException();

            return arr[cursor++];
        }
    }
}
