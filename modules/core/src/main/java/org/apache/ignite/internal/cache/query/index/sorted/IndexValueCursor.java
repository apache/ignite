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

package org.apache.ignite.internal.cache.query.index.sorted;

import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridCursor;

/**
 * Cursor over index values.
 *
 * @param <V> class represents of value stored in an index.
 */
public class IndexValueCursor<V> implements GridCursor<V> {
    /**
     * Empty cursor implementation.
     */
    public static final GridCursor EMPTY = new GridCursor() {
        /** {@inheritDoc} */
        @Override public boolean next() {
            return false;
        }

        /**
         * Note that this implimentation violates the contract of GridCusror. It should throw {@code NoSuchElementException}
         * instead of returning {@code null}. But current implementation of SQL with H2 relies on this {@code null}
         * for queries like "select max(col) from table". This should be fixed for other SQL engines.
         *
         * https://issues.apache.org/jira/browse/IGNITE-14303.
         */
        @Override public Object get() {
            return null;
        }
    };

    /** Underlying cursor over original index rows. */
    private final GridCursor<IndexRow> delegate;

    /** Map function that transforms index row to index value. */
    private final Function<IndexRow, V> mapFunc;

    /** */
    public IndexValueCursor(GridCursor<IndexRow> delegate, Function<IndexRow, V> mapFunc) {
        this.delegate = delegate;
        this.mapFunc = mapFunc;
    }

    /** {@inheritDoc} */
    @Override public boolean next() throws IgniteCheckedException {
        return delegate.next();
    }

    /** {@inheritDoc} */
    @Override public V get() throws IgniteCheckedException {
        return mapFunc.apply(delegate.get());
    }
}
