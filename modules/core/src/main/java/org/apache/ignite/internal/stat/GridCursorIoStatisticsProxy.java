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
 *
 */

package org.apache.ignite.internal.stat;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.lang.GridCursor;

/**
 * Proxy for {@code GridCursor} to add IO statistics.
 */
public class GridCursorIoStatisticsProxy<T> implements GridCursor<T> {

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Cursor delegate. */
    private final GridCursor<T> delegate;

    /**
     * @param delegate Cursor delegate.
     * @param cctx Cache context.
     */
    public GridCursorIoStatisticsProxy(GridCursor<T> delegate, GridCacheContext cctx) {
        assert cctx != null;

        this.delegate = delegate;

        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public boolean next() throws IgniteCheckedException {
        cctx.startGatheringStatistics();

        try {
            return delegate.next();
        }
        finally {
            cctx.finishGatheringStatistics();
        }
    }

    /** {@inheritDoc} */
    @Override public T get() throws IgniteCheckedException {
        return delegate.get();
    }
}
