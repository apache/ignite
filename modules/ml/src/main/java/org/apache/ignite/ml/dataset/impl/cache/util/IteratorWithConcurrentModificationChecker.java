/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator wrapper that checks if number of entries in iterator is equal to expected.
 *
 * @param <T> Type of entries.
 */
public class IteratorWithConcurrentModificationChecker<T> implements Iterator<T> {
    /** Delegate. */
    private final Iterator<T> delegate;

    /** Expected count of entries. */
    private long expCnt;

    /** Exception message. */
    private final String eMsg;

    /**
     * Constructs a new instance of iterator checked wrapper.
     *
     * @param delegate Delegate.
     * @param expCnt Expected count of entries.
     */
    public IteratorWithConcurrentModificationChecker(Iterator<T> delegate, long expCnt, String eMsg) {
        this.delegate = delegate;
        this.expCnt = expCnt;
        this.eMsg = eMsg;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        boolean hasNext = delegate.hasNext();

        if (!hasNext ^ expCnt == 0)
            throw new ConcurrentModificationException(eMsg);

        return hasNext;
    }

    /** {@inheritDoc} */
    @Override public T next() {
        try {
            T next = delegate.next();

            if (expCnt == 0)
                throw new ConcurrentModificationException(eMsg);

            expCnt--;

            return next;
        }
        catch (NoSuchElementException e) {
            if (expCnt == 0)
                throw e;
            else
                throw new ConcurrentModificationException(eMsg);
        }
    }
}