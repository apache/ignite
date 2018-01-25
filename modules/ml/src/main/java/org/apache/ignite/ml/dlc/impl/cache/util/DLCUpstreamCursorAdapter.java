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

package org.apache.ignite.ml.dlc.impl.cache.util;

import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.ml.dlc.DLCUpstreamEntry;

/**
 * Cursor adapter used to transform {@link Cache.Entry} received from Ignite Cache query cursor into DLC-specific
 * {@link DLCUpstreamEntry}.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 */
public class DLCUpstreamCursorAdapter<K, V> implements Iterable<DLCUpstreamEntry<K, V>> {
    /** Ignite Cache cursor. */
    private final QueryCursor<Cache.Entry<K, V>> cursor;

    /**
     * Constructs a new instance of query cursor adapter.
     *
     * @param cursor Ignite Cache query cursor
     */
    public DLCUpstreamCursorAdapter(QueryCursor<Cache.Entry<K, V>> cursor) {
        this.cursor = cursor;
    }

    /** {@inheritDoc} */
    @Override public Iterator<DLCUpstreamEntry<K, V>> iterator() {
        return new DLCUpstreamCursorWrapperIterator(cursor.iterator());
    }

    /**
     * Utils class representing iterator of {@link DLCUpstreamEntry} based on iterator of {@link Cache.Entry}.
     */
    private final class DLCUpstreamCursorWrapperIterator implements Iterator<DLCUpstreamEntry<K, V>> {
        /** Cache entry iterator. */
        private final Iterator<Cache.Entry<K, V>> delegate;

        /**
         * Constructs a new instance of iterator.
         *
         * @param delegate cache entry iterator
         */
        DLCUpstreamCursorWrapperIterator(Iterator<Cache.Entry<K, V>> delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return delegate.hasNext();
        }

        /** {@inheritDoc} */
        @Override public DLCUpstreamEntry<K, V> next() {
            Cache.Entry<K, V> next = delegate.next();

            if (next == null)
                return null;

            return new DLCUpstreamEntry<>(next.getKey(), next.getValue());
        }
    }
}
