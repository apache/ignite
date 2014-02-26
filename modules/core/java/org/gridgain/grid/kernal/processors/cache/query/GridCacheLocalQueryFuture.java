// @java.file.header

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.util.*;

/**
 * Local query future.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheLocalQueryFuture<K, V, R> extends GridCacheQueryFutureAdapter<K, V, R> {
    /** */
    private Runnable run;

    /** */
    private GridFuture<?> fut;

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

                // TODO: gg-7625
//                if (fut.query() instanceof GridCacheFieldsQueryBase)
//                    mgr.runFieldsQuery(localQueryInfo(fut, single, vis));
//                else
                    cctx.queries().runQuery(localQueryInfo());
            }
            catch (Throwable e) {
                onDone(e);
            }
        }

        /**
         * @return Query info.
         */
        @SuppressWarnings({"unchecked"})
        private GridCacheQueryInfo localQueryInfo() {
            GridCacheQueryBean qry = query();

            GridPredicate<GridCacheEntry<Object, Object>> prjPred = qry.query().projectionFilter() == null ?
                F.<GridCacheEntry<Object, Object>>alwaysTrue() : qry.query().projectionFilter();

            GridReducer<Object, Object> rdc = qry.reducer();
            GridClosure<Object, Object> trans = qry.transform();

//            boolean incMeta = false;
//
//            if (qry instanceof GridCacheFieldsQueryBase)
//                incMeta = ((GridCacheFieldsQueryBase)qry).includeMetadata();

            return new GridCacheQueryInfo(
                true,
                prjPred,
                trans,
                rdc,
                qry.query(),
                GridCacheLocalQueryFuture.this,
                null,
                -1,
                false, // TODO: gg-7625
                true,
                qry.arguments()
            );
        }
    }
}
