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

package org.apache.ignite.internal.processors.cache;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.cache.query.QueryCursor;
import org.jetbrains.annotations.NotNull;

/**
 * Cursor wrapper that skips all entires that maps to any of {@code skipKeys} key.
 * <b>Note, for the performance reasons content of {@code skipKeys} will be changed during iteration.</b>
 */
public class KeyFilteringQueryCursor<T> implements QueryCursor<T> {
    /** Underlying cursor. */
    private final QueryCursor<T> cursor;

    /** Keys that must be skiped on {@link #cursor} iteration. */
    private final Set<KeyCacheObject> skipKeys;

    /** Mapper from row to {@link KeyCacheObject}. */
    private final Function<T, KeyCacheObject> toKey;

    /**
     * @param cursor Sorted cursor.
     * @param skipKeys Keys to skip. <b>Content will be changed during iteration.</b>
     */
    public KeyFilteringQueryCursor(QueryCursor<T> cursor, Set<KeyCacheObject> skipKeys, Function<T, KeyCacheObject> toKey) {
        this.cursor = cursor;
        this.skipKeys = skipKeys;
        this.toKey = toKey;
    }

    /** {@inheritDoc} */
    @Override public List<T> getAll() {
        List<T> all = cursor.getAll();

        // Intentionally use of `Set#remove` here.
        // We want perform as few `toKey` as possible.
        // So we break some rules here to optimize work with the data provided by the underlying cursor.
        all.removeIf(e -> !skipKeys.isEmpty() && skipKeys.remove(toKey.apply(e)));

        return all;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        cursor.close();
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Iterator<T> iterator() {
        return null;
    }
}
