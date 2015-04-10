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

import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.*;

/**
 * {@link CacheQueries} implementation.
 */
public class GridCacheQueriesImpl<K, V> implements GridCacheQueriesEx<K, V> {
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
            null,
            null,
            (IgniteBiPredicate<Object, Object>)filter,
            false,
            prj != null && prj.isKeepPortable());
    }

    /**
     * Query for {@link IndexingSpi}.
     *
     * @return Query.
     */
    @Override public <R> CacheQuery<R> createSpiQuery() {
        return new GridCacheQueryAdapter<>(ctx,
            SPI,
            null,
            null,
            null,
            false,
            prj != null && prj.isKeepPortable());
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> execute(String space, GridCacheTwoStepQuery qry) {
        return ctx.kernalContext().query().queryTwoStep(space, qry);
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> executeTwoStepQuery(String space, String sqlQry, Object[] params) {
        return ctx.kernalContext().query().queryTwoStep(ctx, new SqlFieldsQuery(sqlQry).setArgs(params));
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
            null,
            qry,
            null,
            incMeta,
            prj != null && prj.isKeepPortable());
    }
}
