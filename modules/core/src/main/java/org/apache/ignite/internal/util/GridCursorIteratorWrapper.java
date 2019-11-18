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

package org.apache.ignite.internal.util;

import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridCursor;

/**
 * Wrap {@code Iterator} and adapt it to {@code GridCursor}.
 */
public class GridCursorIteratorWrapper<V> implements GridCursor<V> {
    /** Iterator. */
    private Iterator<V> iter;

    /** Next. */
    private V next;

    /**
     * @param iter Iterator.
     */
    public GridCursorIteratorWrapper(Iterator<V> iter) {
        this.iter = iter;
    }

    /** {@inheritDoc} */
    @Override public V get() throws IgniteCheckedException {
        return next;
    }

    /** {@inheritDoc} */
    @Override public boolean next() throws IgniteCheckedException {
        next = iter.hasNext() ? iter.next() : null;

        return next != null;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        // No-op.
    }
}
