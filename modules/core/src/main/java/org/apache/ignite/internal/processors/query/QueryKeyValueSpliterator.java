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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.internal.processors.cache.CacheEntryImpl;

import javax.cache.Cache;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * SqlQuery key-value spliterator.
 */
public class QueryKeyValueSpliterator<K, V> implements Spliterator<Cache.Entry<K, V>> {
    /** Target spliterator. */
    private final Spliterator<List<?>> spliterator;

    /**
     * Constructor.
     *
     * @param spliterator Target spliterator.
     */
    public QueryKeyValueSpliterator(Spliterator<List<?>> spliterator) {
        this.spliterator = spliterator;
    }

    /** {@inheritDoc} */
    @Override public boolean tryAdvance(Consumer<? super Cache.Entry<K, V>> action) {
        return spliterator.tryAdvance(new Consumer<List<?>>() {
            @Override public void accept(List<?> row) {
                action.accept(new CacheEntryImpl<>((K)row.get(0), (V)row.get(1)));
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Spliterator<Cache.Entry<K, V>> trySplit() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long estimateSize() {
        return spliterator.estimateSize();
    }

    /** {@inheritDoc} */
    @Override public int characteristics() {
        return spliterator.characteristics();
    }
}
