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
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;

import java.io.*;
import java.util.*;

/**
 * Local query future.
 */
public class GridCacheLocalQueryFuture<K, V, R> extends GridCacheQueryFutureAdapter<K, V, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private Runnable run;

    /** */
    private IgniteFuture<?> fut;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheLocalQueryFuture() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param qry Query.
     */
    protected GridCacheLocalQueryFuture(GridCacheContext<K, V> ctx, GridCacheQueryBean qry) {
        super(ctx, qry, true);

        run = new LocalQueryRunnable<>();
    }

    /**
     * Executes query runnable.
     */
    void execute() {
        fut = ctx.closure().runLocalSafe(run, true);
    }

    /** {@inheritDoc} */
    @Override protected void cancelQuery() throws IgniteCheckedException {
        if (fut != null)
            fut.cancel();
    }

    /** {@inheritDoc} */
    @Override protected boolean onPage(UUID nodeId, boolean last) {
        return last;
    }

    /** {@inheritDoc} */
    @Override protected void loadPage() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void loadAllPages() {
        // No-op.
    }

    /** */
    private class LocalQueryRunnable<K, V, R> implements GridPlainRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            try {
                qry.query().validate();

                if (fields())
                    cctx.queries().runFieldsQuery(localQueryInfo());
                else
                    cctx.queries().runQuery(localQueryInfo());
            }
            catch (Throwable e) {
                onDone(e);
            }
        }

        /**
         * @return Query info.
         * @throws IgniteCheckedException In case of error.
         */
        @SuppressWarnings({"unchecked"})
        private GridCacheQueryInfo localQueryInfo() throws IgniteCheckedException {
            GridCacheQueryBean qry = query();

            IgnitePredicate<GridCacheEntry<Object, Object>> prjPred = qry.query().projectionFilter() == null ?
                F.<GridCacheEntry<Object, Object>>alwaysTrue() : qry.query().projectionFilter();

            IgniteMarshaller marsh = cctx.marshaller();

            IgniteReducer<Object, Object> rdc = qry.reducer() != null ?
                marsh.<IgniteReducer<Object, Object>>unmarshal(marsh.marshal(qry.reducer()), null) : null;

            IgniteClosure<Object, Object> trans = qry.transform() != null ?
                marsh.<IgniteClosure<Object, Object>>unmarshal(marsh.marshal(qry.transform()), null) : null;

            return new GridCacheQueryInfo(
                true,
                prjPred,
                trans,
                rdc,
                qry.query(),
                GridCacheLocalQueryFuture.this,
                ctx.localNodeId(),
                cctx.io().nextIoId(),
                qry.query().includeMetadata(),
                true,
                qry.arguments()
            );
        }
    }
}
