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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.internal.processors.cache.query.QueryCursorEx;

import java.util.Iterator;

/**
 * Query cursor holder.
  */
public class ClientCacheQueryCursor<T> {
    /** Cursor. */
    private final QueryCursorEx<T> cursor;

    /** Page size. */
    private final int pageSize;

    /** Iterator. */
    private Iterator<T> iterator;

    /**
     * Ctor.
     *
     * @param cursor Cursor.
     * @param pageSize Page size.
     */
    ClientCacheQueryCursor(QueryCursorEx<T> cursor, int pageSize) {
        assert cursor != null;
        assert pageSize > 0;

        this.cursor = cursor;
        this.pageSize = pageSize;
    }

    /**
     * Gets the cursor.
     *
     * @return Cursor.
     */
    public QueryCursorEx<T> cursor() {
        return cursor;
    }

    /**
     * Gets the page size.
     *
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * Gets the iterator.
     *
     * @return Iterator.
     */
    public Iterator<T> iterator() {
        if (iterator == null) {
            iterator = cursor.iterator();
        }

        return iterator;
    }
}
