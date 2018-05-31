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

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.cache.Cache;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.dataset.UpstreamEntry;

/**
 * Cursor adapter used to transform {@code Cache.Entry} received from Ignite Cache query cursor into DLC-specific
 * {@link UpstreamEntry}.
 *
 * @param <K> Type of an upstream value key.
 * @param <V> Type of an upstream value.
 */
public class UpstreamCursorAdapter<K, V> implements Iterator<UpstreamEntry<K, V>> {
    /** Cache entry iterator. */
    private final Iterator<Cache.Entry<K, V>> delegate;

    /** Predicate that filters {@code upstream} data. */
    private final IgniteBiPredicate<K, V> pred;

    /** Next entry returned by the delegate iterator. */
    private Cache.Entry<K, V> nextEntry;

    /**
     * Constructs a new instance of iterator.
     *
     * @param delegate Cache entry iterator.
     * @param pred Predicate that filters {@code upstream} data.
     */
    UpstreamCursorAdapter(Iterator<Cache.Entry<K, V>> delegate, IgniteBiPredicate<K, V> pred) {
        this.delegate = delegate;
        this.pred = pred;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        findNext();

        return nextEntry != null;
    }

    /** {@inheritDoc} */
    @Override public UpstreamEntry<K, V> next() {
        if (!hasNext())
            throw new NoSuchElementException();

        Cache.Entry<K, V> next = nextEntry;

        nextEntry = null;

        return new UpstreamEntry<>(next.getKey(), next.getValue());
    }

    /** Finds a next entry returned by delegate iterator. */
    private void findNext() {
        while (nextEntry == null && delegate.hasNext()) {
            Cache.Entry<K, V> entry = delegate.next();

            if (pred.apply(entry.getKey(), entry.getValue()))
                nextEntry = entry;
        }
    }
}
