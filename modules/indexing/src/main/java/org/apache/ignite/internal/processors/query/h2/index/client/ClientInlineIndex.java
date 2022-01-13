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

package org.apache.ignite.internal.processors.query.h2.index.client;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.jetbrains.annotations.Nullable;

/**
 * We need indexes on non-affinity nodes. This index does not contain any data.
 */
public class ClientInlineIndex implements InlineIndex {
    /** */
    private final int inlineSize;

    /** Index name. */
    private final String name;

    /** Index id. */
    private final UUID id = UUID.randomUUID();

    /** */
    public ClientInlineIndex(String idxName, int inlineSize) {
        name = idxName;
        this.inlineSize = inlineSize;
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return inlineSize;
    }

    /** {@inheritDoc} */
    @Override public boolean created() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public InlineIndexTree segment(int segment) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> find(
        IndexRow lower,
        IndexRow upper,
        boolean lowIncl,
        boolean upIncl,
        int segment,
        IndexQueryContext qryCtx
    ) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findFirst(int segment, IndexQueryContext qryCtx) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findLast(int segment, IndexQueryContext qryCtx) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public long count(int segment) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public long totalCount() throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public long count(int segment, IndexQueryContext qryCtx) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public boolean canHandle(CacheDataRow row) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(@Nullable CacheDataRow oldRow, @Nullable CacheDataRow newRow,
        boolean prevRowAvailable) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public <T extends Index> T unwrap(Class<T> clazz) {
        if (clazz == null)
            return null;

        if (clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException(
            String.format("Cannot unwrap [%s] to [%s]", getClass().getName(), clazz.getName())
        );
    }

    /** {@inheritDoc} */
    @Override public void destroy(boolean softDelete) {
        // No-op.
    }

    /**
     * @return Exception about unsupported operation.
     */
    private static IgniteException unsupported() {
        return new IgniteSQLException("Shouldn't be invoked on non-affinity node.");
    }
}
