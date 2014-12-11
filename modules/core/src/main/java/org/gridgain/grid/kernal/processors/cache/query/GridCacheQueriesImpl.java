/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.indexing.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.cache.query.GridCacheQueryType.*;

/**
 * {@link GridCacheQueries} implementation.
 */
public class GridCacheQueriesImpl<K, V> implements GridCacheQueriesEx<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridCacheContext<K, V> ctx;

    /** */
    private GridCacheProjectionImpl<K, V> prj;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueriesImpl() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param prj Projection.
     */
    public GridCacheQueriesImpl(GridCacheContext<K, V> ctx, @Nullable GridCacheProjectionImpl<K, V> prj) {
        assert ctx != null;

        this.ctx = ctx;
        this.prj = prj;
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<Map.Entry<K, V>> createSqlQuery(Class<?> cls, String clause) {
        A.notNull(cls, "cls");
        A.notNull(clause, "clause");

        return new GridCacheQueryAdapter<>(ctx,
            SQL,
            filter(),
            ctx.kernalContext().query().typeName(U.box(cls)),
            clause,
            null,
            false,
            prj != null && prj.isKeepPortable());
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<Map.Entry<K, V>> createSqlQuery(String clsName, String clause) {
        A.notNull("clsName", clsName);
        A.notNull("clause", clause);

        return new GridCacheQueryAdapter<>(ctx,
            SQL,
            filter(),
            clsName,
            clause,
            null,
            false,
            prj != null && prj.isKeepPortable());
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<List<?>> createSqlFieldsQuery(String qry) {
        A.notNull(qry, "qry");

        return new GridCacheQueryAdapter<>(ctx,
            SQL_FIELDS,
            filter(),
            null,
            qry,
            null,
            false,
            prj != null && prj.isKeepPortable());
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<Map.Entry<K, V>> createFullTextQuery(Class<?> cls, String search) {
        A.notNull(cls, "cls");
        A.notNull(search, "search");

        return new GridCacheQueryAdapter<>(ctx,
            TEXT,
            filter(),
            ctx.kernalContext().query().typeName(U.box(cls)),
            search,
            null,
            false,
            prj != null && prj.isKeepPortable());
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<Map.Entry<K, V>> createFullTextQuery(String clsName, String search) {
        A.notNull("clsName", clsName);
        A.notNull("search", search);

        return new GridCacheQueryAdapter<>(ctx,
            TEXT,
            filter(),
            clsName,
            search,
            null,
            false,
            prj != null && prj.isKeepPortable());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public GridCacheQuery<Map.Entry<K, V>> createScanQuery(@Nullable IgniteBiPredicate<K, V> filter) {
        return new GridCacheQueryAdapter<>(ctx,
            SCAN,
            filter(),
            null,
            null,
            (IgniteBiPredicate<Object, Object>)filter,
            false,
            prj != null && prj.isKeepPortable());
    }

    /**
     * Query for {@link GridIndexingSpi}.
     *
     * @return Query.
     */
    public <R> GridCacheQuery<R> createSpiQuery() {
        return new GridCacheQueryAdapter<>(ctx,
            SPI,
            filter(),
            null,
            null,
            null,
            false,
            prj != null && prj.isKeepPortable());
    }

    /** {@inheritDoc} */
    @Override public GridCacheContinuousQuery<K, V> createContinuousQuery() {
        return ctx.continuousQueries().createQuery(prj == null ? null : prj.predicate());
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> rebuildIndexes(Class<?> cls) {
        A.notNull(cls, "cls");

        return ctx.queries().rebuildIndexes(cls);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> rebuildIndexes(String typeName) {
        A.notNull("typeName", typeName);

        return ctx.queries().rebuildIndexes(typeName);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> rebuildAllIndexes() {
        return ctx.queries().rebuildAllIndexes();
    }

    /** {@inheritDoc} */
    @Override public GridCacheQueryMetrics metrics() {
        return ctx.queries().metrics();
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        ctx.queries().resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheSqlMetadata> sqlMetadata() throws IgniteCheckedException {
        return ctx.queries().sqlMetadata();
    }

    /** {@inheritDoc} */
    @Override public GridCacheQuery<List<?>> createSqlFieldsQuery(String qry, boolean incMeta) {
        assert qry != null;

        return new GridCacheQueryAdapter<>(ctx,
            SQL_FIELDS,
            filter(),
            null,
            qry,
            null,
            incMeta,
            prj != null && prj.isKeepPortable());
    }

    /**
     * @return Optional projection filter.
     */
    @SuppressWarnings("unchecked")
    @Nullable private IgnitePredicate<GridCacheEntry<Object, Object>> filter() {
        return prj == null ? null : ((GridCacheProjectionImpl<Object, Object>)prj).predicate();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(prj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        prj = (GridCacheProjectionImpl<K, V>)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    private Object readResolve() throws ObjectStreamException {
        return prj.queries();
    }
}
