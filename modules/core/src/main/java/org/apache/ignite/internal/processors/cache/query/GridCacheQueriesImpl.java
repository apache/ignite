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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.indexing.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.*;

/**
 * {@link CacheQueries} implementation.
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
    @Override public CacheQuery<Map.Entry<K, V>> createSqlQuery(Class<?> cls, String clause) {
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
    @Override public CacheQuery<Map.Entry<K, V>> createSqlQuery(String clsName, String clause) {
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
    @Override public CacheQuery<List<?>> createSqlFieldsQuery(String qry) {
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
    @Override public CacheQuery<Map.Entry<K, V>> createFullTextQuery(Class<?> cls, String search) {
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
    @Override public CacheQuery<Map.Entry<K, V>> createFullTextQuery(String clsName, String search) {
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
    @Override public CacheQuery<Map.Entry<K, V>> createScanQuery(@Nullable IgniteBiPredicate<K, V> filter) {
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
    public <R> CacheQuery<R> createSpiQuery() {
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
    @Override public IgniteInternalFuture<GridCacheSqlResult> execute(String space, GridCacheTwoStepQuery qry) {
        return ctx.kernalContext().query().queryTwoStep(space, qry);
    }

    /**
     * @param space Space.
     * @param sqlQry Query.
     * @param params Parameters.
     * @return Result.
     */
    public IgniteInternalFuture<GridCacheSqlResult> executeTwoStepQuery(String space, String sqlQry, Object[] params) {
        return ctx.kernalContext().query().queryTwoStep(space, sqlQry, params);
    }

    /** {@inheritDoc} */
    @Override public CacheContinuousQuery<K, V> createContinuousQuery() {
        return ctx.continuousQueries().createQuery(prj == null ? null : prj.predicate());
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebuildIndexes(Class<?> cls) {
        A.notNull(cls, "cls");

        return ctx.queries().rebuildIndexes(cls);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebuildIndexes(String typeName) {
        A.notNull("typeName", typeName);

        return ctx.queries().rebuildIndexes(typeName);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebuildAllIndexes() {
        return ctx.queries().rebuildAllIndexes();
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics metrics() {
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
    @Override public CacheQuery<List<?>> createSqlFieldsQuery(String qry, boolean incMeta) {
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
    @Nullable private IgnitePredicate<Cache.Entry<Object, Object>> filter() {
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
