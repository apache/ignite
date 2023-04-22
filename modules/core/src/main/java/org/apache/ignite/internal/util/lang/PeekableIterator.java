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
import java.util.NoSuchElementException;

/**
 * Iterator extension that adds {@link #peek()} method to get element without moving to the next.
 */
public class PeekableIterator<T1> implements Iterator<T1> {
    /** Underlying iterator. */
    private final Iterator<T1> iter;

    /** Peeked element. */
    private T1 peeked;

    /**
     * @param iter Underlying iterator.
     */
    public PeekableIterator(Iterator<T1> iter) {
        this.iter = iter;
    }

    /**
     * Peek and return the next element in the iteration.
     *
     * @return the next element in the iteration.
     * @throws NoSuchElementException if the iteration has no more elements.
     */
    public T1 peek() {
        return peeked != null ? peeked : (peeked = next());
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return peeked != null || iter.hasNext();
    }

    /** {@inheritDoc} */
    @Override public T1 next() {
        if (peeked != null) {
            T1 peeked0 = peeked;

            peeked = null;

            return peeked0;
        }

        return iter.next();
    }
}
