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

package org.apache.ignite.ml.dlc.impl.local.util;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.dlc.DLCUpstreamEntry;

/**
 * This is {@code Map} adapter used to transform {@code Map} entries into DLC-specific {@link DLCUpstreamEntry}.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 */
public class DLCUpstreamMapAdapter<K, V> implements Iterable<DLCUpstreamEntry<K, V>> {
    /** Upstream data {@code Map}. */
    private final Map<K, V> upstreamData;

    /** Ordered upstream data keys. */
    private final List<K> keys;

    /**
     * Constructs a new instance of {@code Map} adapter.
     *
     * @param upstreamData upstream data {@code Map}
     * @param keys ordered upstream data keys
     */
    public DLCUpstreamMapAdapter(Map<K, V> upstreamData, List<K> keys) {
        this.upstreamData = upstreamData;
        this.keys = keys;
    }

    /** {@inheritDoc} */
    @Override public Iterator<DLCUpstreamEntry<K, V>> iterator() {
        return new DLCUpstreamKeysWrapperIterator(keys.iterator());
    }

    /**
     * Utils class representing iterator of {@link DLCUpstreamEntry} based on iterator of {@code Map} keys.
     */
    private final class DLCUpstreamKeysWrapperIterator implements Iterator<DLCUpstreamEntry<K, V>> {
        /** Keys iterator. */
        private final Iterator<K> keysIterator;

        /**
         * Constructs a new instance of iterator.
         *
         * @param keysIterator keys iterator
         */
        DLCUpstreamKeysWrapperIterator(Iterator<K> keysIterator) {
            this.keysIterator = keysIterator;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return keysIterator.hasNext();
        }

        /** {@inheritDoc} */
        @Override public DLCUpstreamEntry<K, V> next() {
            K nextKey = keysIterator.next();

            if (nextKey == null)
                return null;

            return new DLCUpstreamEntry<>(nextKey, upstreamData.get(nextKey));
        }
    }
}
