/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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