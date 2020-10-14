package org.apache.ignite.internal.processors.query.h2.index.client;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.cache.query.index.sorted.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;

/**
 * We need indexes on an not affinity nodes. The index shouldn't contains any data.
 */
public class ClientInlineIndex implements InlineIndex {
    /** */
    private final int inlineSize;

    /** */
    public ClientInlineIndex(IndexKeyDefinition[] keyDefs, int cfgInlineSize, int maxInlineSize) {
        inlineSize = InlineIndexTree.computeInlineSize(keyDefs, cfgInlineSize, maxInlineSize);
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return inlineSize;
    }

    /** {@inheritDoc} */
    @Override public boolean isCreated() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexSearchRow> find(IndexKey lower, IndexKey upper, int segment) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexSearchRow> find(IndexKey lower, IndexKey upper, int segment,
        IndexingQueryFilter filter) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexSearchRow> findFirstOrLast(boolean firstOrLast, int segment,
        IndexingQueryFilter filter) throws IgniteCheckedException {
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
    @Override public long count(int segment, IndexingQueryFilter filter) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(@Nullable CacheDataRow oldRow, @Nullable CacheDataRow newRow) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow put(CacheDataRow row) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean putx(CacheDataRow row) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean removex(CacheDataRow row) throws IgniteCheckedException {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public <T extends Index> T unwrap(Class<T> clazz) {
        throw unsupported();
    }

    /** {@inheritDoc} */
    @Override public void destroy(boolean softDelete) {
        throw unsupported();
    }

    /**
     * @return Exception about unsupported operation.
     */
    private static IgniteException unsupported() {
        return new IgniteSQLException("Shouldn't be invoked on non-affinity node.");
    }
}
