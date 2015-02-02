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
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Per-projection queries object returned to user.
 */
public class GridCacheQueriesProxy<K, V> implements GridCacheQueriesEx<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridCacheGateway<K, V> gate;

    /** */
    private GridCacheProjectionImpl<K, V> prj;

    /** */
    private GridCacheQueriesEx<K, V> delegate;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueriesProxy() {
        // No-op.
    }

    /**
     * Create cache queries implementation.
     *
     * @param cctx Сontext.
     * @param prj Optional cache projection.
     * @param delegate Delegate object.
     */
    public GridCacheQueriesProxy(GridCacheContext<K, V> cctx, @Nullable GridCacheProjectionImpl<K, V> prj,
        GridCacheQueriesEx<K, V> delegate) {
        assert cctx != null;
        assert delegate != null;

        gate = cctx.gate();

        this.prj = prj;
        this.delegate = delegate;
    }

    /**
     * Gets cache projection.
     *
     * @return Cache projection.
     */
    public CacheProjection<K, V> projection() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public CacheQuery<Map.Entry<K, V>> createSqlQuery(Class<?> cls, String clause) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createSqlQuery(cls, clause);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheQuery<Map.Entry<K, V>> createSqlQuery(String clsName, String clause) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createSqlQuery(clsName, clause);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheQuery<List<?>> createSqlFieldsQuery(String qry) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createSqlFieldsQuery(qry);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheQuery<Map.Entry<K, V>> createFullTextQuery(Class<?> cls, String search) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createFullTextQuery(cls, search);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheQuery<Map.Entry<K, V>> createFullTextQuery(String clsName, String search) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createFullTextQuery(clsName, search);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheQuery<Map.Entry<K, V>> createScanQuery(@Nullable IgniteBiPredicate<K, V> filter) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createScanQuery(filter);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheContinuousQuery<K, V> createContinuousQuery() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createContinuousQuery();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> CacheQuery<R> createSpiQuery() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createSpiQuery();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> execute(String space, GridCacheTwoStepQuery qry) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.execute(space, qry);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> executeTwoStepQuery(String space, String sqlQry, Object[] params) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.executeTwoStepQuery(space, sqlQry, params);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebuildIndexes(Class<?> cls) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.rebuildIndexes(cls);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebuildIndexes(String typeName) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.rebuildIndexes(typeName);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> rebuildAllIndexes() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.rebuildAllIndexes();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics metrics() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.metrics();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            delegate.resetMetrics();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheSqlMetadata> sqlMetadata() throws IgniteCheckedException {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.sqlMetadata();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheQuery<List<?>> createSqlFieldsQuery(String qry, boolean incMeta) {
        GridCacheProjectionImpl<K, V> prev = gate.enter(prj);

        try {
            return delegate.createSqlFieldsQuery(qry, incMeta);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(prj);
        out.writeObject(delegate);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        prj = (GridCacheProjectionImpl<K, V>)in.readObject();
        delegate = (GridCacheQueriesEx<K, V>)in.readObject();

        gate = prj.context().gate();
    }
}
