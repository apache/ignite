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
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Per-projection queries object returned to user.
 */
public class CacheQueriesProxy<K, V> implements CacheQueries<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridCacheGateway<K, V> gate;

    /** */
    private GridCacheContext<K, V> cctx;

    /** */
    private CacheOperationContext prj;

    /** */
    private CacheQueries<K, V> delegate;

    /**
     * Required by {@link Externalizable}.
     */
    public CacheQueriesProxy() {
        // No-op.
    }

    /**
     * Create cache queries implementation.
     *
     * @param cctx Context.
     * @param prj Optional cache projection.
     * @param delegate Delegate object.
     */
    public CacheQueriesProxy(GridCacheContext<K, V> cctx, @Nullable CacheOperationContext prj,
        CacheQueries<K, V> delegate) {
        assert cctx != null;
        assert delegate != null;

        this.cctx = cctx;
        gate = cctx.gate();

        this.prj = prj;
        this.delegate = delegate;
    }

    /**
     * Gets cache projection.
     *
     * @return Cache projection.
     */
    public CacheOperationContext projection() {
        return prj;
    }

    /** {@inheritDoc} */
    @Override public CacheQuery<List<?>> createSqlFieldsQuery(String qry) {
        CacheOperationContext prev = gate.enter(prj);

        try {
            return delegate.createSqlFieldsQuery(qry);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheQuery<Map.Entry<K, V>> createFullTextQuery(String clsName, String search) {
        CacheOperationContext prev = gate.enter(prj);

        try {
            return delegate.createFullTextQuery(clsName, search);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheQuery<Map.Entry<K, V>> createScanQuery(@Nullable IgniteBiPredicate<K, V> filter) {
        CacheOperationContext prev = gate.enter(prj);

        try {
            return delegate.createScanQuery(filter);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public <R> CacheQuery<R> createSpiQuery() {
        CacheOperationContext prev = gate.enter(prj);

        try {
            return delegate.createSpiQuery();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> execute(String space, GridCacheTwoStepQuery qry) {
        CacheOperationContext prev = gate.enter(prj);

        try {
            return delegate.execute(space, qry);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryCursor<List<?>> executeTwoStepQuery(String space, String sqlQry, Object[] params) {
        CacheOperationContext prev = gate.enter(prj);

        try {
            return delegate.executeTwoStepQuery(space, sqlQry, params);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public QueryMetrics metrics() {
        CacheOperationContext prev = gate.enter(prj);

        try {
            return delegate.metrics();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheSqlMetadata> sqlMetadata() throws IgniteCheckedException {
        CacheOperationContext prev = gate.enter(prj);

        try {
            return delegate.sqlMetadata();
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public CacheQuery<List<?>> createSqlFieldsQuery(String qry, boolean incMeta) {
        CacheOperationContext prev = gate.enter(prj);

        try {
            return delegate.createSqlFieldsQuery(qry, incMeta);
        }
        finally {
            gate.leave(prev);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cctx);
        out.writeObject(prj);
        out.writeObject(delegate);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cctx = (GridCacheContext<K,V>)in.readObject();
        prj = (CacheOperationContext)in.readObject();
        delegate = (CacheQueries<K, V>)in.readObject();

        gate = cctx.gate();
    }
}
