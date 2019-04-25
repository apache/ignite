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

package org.apache.ignite.internal.processors.cache;

import java.util.Iterator;
import org.apache.ignite.cache.query.QueryCursor;

/**
 * Implementation of iterator wrapper to close cursor when all data has been read from iterator.
 *
 * @param <T> The type of elements returned by this iterator.
 */
class AutoClosableCursorIterator<T> implements Iterator<T> {
    /** Cursor. */
    private final QueryCursor cursor;

    /** Iterator. */
    private final Iterator<T> iter;

    /**
     * Constructor.
     *
     * @param cursor Query cursor.
     * @param iter Wrapped iterator.
     */
    public AutoClosableCursorIterator(QueryCursor cursor, Iterator<T> iter) {
        this.cursor = cursor;
        this.iter = iter;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        boolean hasNext = iter.hasNext();

        if (!hasNext)
            cursor.close();

        return hasNext;
    }

    /** {@inheritDoc} */
    @Override public T next() {
        return iter.next();
    }
}
