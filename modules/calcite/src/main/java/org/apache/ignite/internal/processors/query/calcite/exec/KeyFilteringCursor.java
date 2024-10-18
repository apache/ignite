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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Set;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.lang.GridCursor;

/**
 * Cursor wrapper that skips all entires that maps to any of {@code skipKeys} key.
 * <b>Note, for the performance reasons content of {@code skipKeys} will be changed during iteration.</b>
 */
class KeyFilteringCursor<R> implements GridCursor<R> {
    /** Underlying cursor. */
    private final GridCursor<? extends R> cursor;

    /** Rows that must be skiped on {@link #cursor} iteration. */
    private final Set<KeyCacheObject> skipKeys;

    /** Mapper from row to {@link KeyCacheObject}. */
    private final Function<R, KeyCacheObject> toKey;

    /**
     * @param cursor Sorted cursor.
     * @param skipKeys Keys to skip. <b>Content will be changed during iteration.</b>
     * @param toKey Mapper from row to {@link KeyCacheObject}.
     */
    KeyFilteringCursor(GridCursor<? extends R> cursor, Set<KeyCacheObject> skipKeys, Function<R, KeyCacheObject> toKey) {
        this.cursor = cursor;
        this.skipKeys = skipKeys;
        this.toKey = toKey;
    }

    /** {@inheritDoc} */
    @Override public boolean next() throws IgniteCheckedException {
        R cur;

        do {
            if (!cursor.next())
                return false;

            cur = cursor.get();

            // Intentionally use of `Set#remove` here.
            // We want perform as few `toKey` as possible.
            // So we break some rules here to optimize work with the data provided by the underlying cursor.
        } while (!skipKeys.isEmpty() && skipKeys.remove(toKey.apply(cur)));

        return true;
    }

    /** {@inheritDoc} */
    @Override public R get() throws IgniteCheckedException {
        return cursor.get();
    }
}
