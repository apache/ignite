/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.future;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.lang.*;

/**
 * Future which asynchronously resolves result from futures chained like {@code Future<Future<...Future<X>>>}.
 */
@SuppressWarnings("unchecked")
public class GridChainedFuture<X> extends GridFutureAdapter<X> implements GridInClosure<GridFuture<?>> {
    /** Future completion callback. */
    private GridClosure<?, X> completionCb;

    /**
     *
     */
    public GridChainedFuture() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     */
    public GridChainedFuture(GridKernalContext ctx) {
        super(ctx);
    }

    public GridChainedFuture(GridKernalContext ctx, GridClosure<?, X> completionCb) {
        super(ctx);

        this.completionCb = completionCb;
    }

    /**
     * @param ctx Kernal context.
     * @param syncNotify Synchronous notify flag.
     */
    public GridChainedFuture(GridKernalContext ctx, boolean syncNotify) {
        super(ctx, syncNotify);
    }

    /** {@inheritDoc} */
    @Override public void apply(GridFuture<?> fut) {
        try {
            Object res = fut.get();

            if (res instanceof GridFuture)
                ((GridFuture)res).listenAsync(this);
            else {
                GridClosure<Object, X> cb = (GridClosure<Object, X>)completionCb;

                if (cb == null)
                    onDone((X)res);
                else
                    onDone(cb.apply(res));
            }
        }
        catch (Throwable e) {
            onDone(e);
        }
    }
}
