/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.query;

import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;

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
    @Override protected void cancelQuery() throws GridException {
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
         * @throws GridException In case of error.
         */
        @SuppressWarnings({"unchecked"})
        private GridCacheQueryInfo localQueryInfo() throws GridException {
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
