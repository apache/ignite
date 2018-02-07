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

    /** Size. */
    private long cnt;

    /**
     * Constructs a new instance of iterator.
     *
     * @param delegate Cache entry iterator.
     */
    UpstreamCursorAdapter(Iterator<Cache.Entry<K, V>> delegate, long cnt) {
        this.delegate = delegate;
        this.cnt = cnt;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return delegate.hasNext() && cnt > 0;
    }

    /** {@inheritDoc} */
    @Override public UpstreamEntry<K, V> next() {
        if (cnt == 0)
            throw new NoSuchElementException();

        cnt--;

        Cache.Entry<K, V> next = delegate.next();

        if (next == null)
            return null;

        return new UpstreamEntry<>(next.getKey(), next.getValue());
    }
}
