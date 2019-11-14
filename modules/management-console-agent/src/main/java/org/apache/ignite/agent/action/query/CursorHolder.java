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

package org.apache.ignite.agent.action.query;

import java.util.Iterator;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Cursor holder.
 */
public class CursorHolder implements AutoCloseable, Iterator {
    /** Cursor. */
    private final QueryCursor cursor;

    /** Iterator. */
    private final Iterator iter;

    /** Is scan cursor. */
    private boolean isScanCursor;

    /**
     * @param cursor Cursor.
     */
    public CursorHolder(QueryCursor cursor) {
        this(cursor, false);
    }

    /**
     * @param cursor Cursor.
     */
    public CursorHolder(QueryCursor cursor, boolean isScanCursor) {
        this.cursor = cursor;
        this.isScanCursor = isScanCursor;
        this.iter = cursor.iterator();
    }

    /**
     * @return @{code true} if this cursor from scan query.
     */
    public boolean scanCursor() {
        return isScanCursor;
    }

    /**
     * @param scanCursor Scan cursor.
     * @return This for chaining methods.
     */
    public CursorHolder scanCursor(boolean scanCursor) {
        isScanCursor = scanCursor;

        return this;
    }

    /**
     * @return Query cursor.
     */
    public QueryCursor cursor() {
        return cursor;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(cursor);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return iter.hasNext();
    }

    /** {@inheritDoc} */
    @Override public Object next() {
        return iter.next();
    }
}
