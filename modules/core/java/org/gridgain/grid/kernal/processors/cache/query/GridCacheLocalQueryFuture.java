// @java.file.header

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

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
     * @param single Single result or not.
     * @param rmtRdcOnly {@code true} for reduce query when using remote reducer only,
     *      otherwise it is always {@code false}.
     * @param pageLsnr Page listener.
     * @param vis Visitor predicate.
     */
    protected GridCacheLocalQueryFuture(GridCacheContext<K, V> ctx, GridCacheQueryBaseAdapter<K, V> qry,
        boolean single, boolean rmtRdcOnly, @Nullable GridBiInClosure<UUID, Collection<R>> pageLsnr,
        @Nullable GridPredicate<?> vis) {
        super(ctx, qry, true, single, rmtRdcOnly, pageLsnr);

        run = new LocalQueryRunnable<>(ctx.queries(), this, single, vis);
    }

    /**
     * Executes query runnable.
     *
     * @param sync Whether to execute synchronously.
     */
    void execute(boolean sync) {
        if (sync)
            run.run();
        else
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
}
