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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.reducer.CacheQueryReducer;
import org.apache.ignite.internal.processors.cache.query.reducer.NodePageStream;
import org.apache.ignite.internal.processors.cache.query.reducer.UnsortedCacheQueryReducer;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.marshaller.Marshaller;

/**
 * Local query future.
 */
public class GridCacheLocalQueryFuture<K, V, R> extends GridCacheQueryFutureAdapter<K, V, R> {
    /** */
    private Runnable run;

    /** */
    private IgniteInternalFuture<?> fut;

    /** Local node page stream with single page. */
    private final NodePageStream<R> stream;

    /**
     * @param ctx Context.
     * @param qry Query.
     */
    protected GridCacheLocalQueryFuture(GridCacheContext<K, V> ctx, GridCacheQueryBean qry) {
        super(ctx, qry, true);

        run = new LocalQueryRunnable();

        stream = new NodePageStream<>(ctx.localNodeId(), () -> {}, () -> {});

        reducer = new UnsortedCacheQueryReducer<>(F.asMap(stream.nodeId(), stream));
    }

    /**
     * Executes query runnable.
     */
    void execute() {
        fut = cctx.kernalContext().closure().runLocalSafe(run, GridIoPolicy.QUERY_POOL);
    }

    /** {@inheritDoc} */
    @Override protected void cancelQuery(Throwable err) throws IgniteCheckedException {
        if (fut != null)
            fut.cancel();

        stream.cancel(err);
    }

    /** {@inheritDoc} */
    @Override public void awaitFirstItemAvailable() throws IgniteCheckedException {
        CacheQueryReducer.get(stream.headPage());
    }

    /** {@inheritDoc} */
    @Override protected void onError(Throwable err) {
        if (onDone(err))
            stream.cancel(err);
    }

    /** {@inheritDoc} */
    @Override protected void onPage(UUID nodeId, Collection<R> data, boolean lastPage) {
        stream.addPage(data, lastPage);
    }

    /** */
    private class LocalQueryRunnable implements GridPlainRunnable {
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

                if (e instanceof Error)
                    throw (Error)e;
            }
        }

        /**
         * @return Query info.
         * @throws IgniteCheckedException In case of error.
         */
        private GridCacheQueryInfo localQueryInfo() throws IgniteCheckedException {
            GridCacheQueryBean qry = query();

            Marshaller marsh = cctx.marshaller();

            IgniteReducer<Object, Object> rdc = qry.reducer() != null ?
                U.<IgniteReducer<Object, Object>>unmarshal(marsh, U.marshal(marsh, qry.reducer()),
                    U.resolveClassLoader(cctx.gridConfig())) : null;

            IgniteClosure<Object, Object> trans = qry.transform() != null ?
                U.<IgniteClosure<Object, Object>>unmarshal(marsh, U.marshal(marsh, qry.transform()),
                    U.resolveClassLoader(cctx.gridConfig())) : null;

            return new GridCacheQueryInfo(
                true,
                trans,
                rdc,
                qry.query(),
                GridCacheLocalQueryFuture.this,
                cctx.localNodeId(),
                cctx.io().nextIoId(),
                qry.query().includeMetadata(),
                true,
                qry.arguments()
            );
        }
    }
}
