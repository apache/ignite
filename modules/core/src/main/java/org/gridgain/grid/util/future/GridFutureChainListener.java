/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.future;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;

/**
 * Future listener to fill chained future with converted result of the source future.
 */
public class GridFutureChainListener<T, R> implements IgniteInClosure<GridFuture<T>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Context. */
    private final GridKernalContext ctx;

    /** Target future. */
    private final GridFutureAdapter<R> fut;

    /** Done callback. */
    private final IgniteClosure<? super GridFuture<T>, R> doneCb;

    /**
     * Constructs chain listener.
     *
     * @param ctx Kernal context.
     * @param fut Target future.
     * @param doneCb Done callback.
     */
    public GridFutureChainListener(GridKernalContext ctx, GridFutureAdapter<R> fut,
        IgniteClosure<? super GridFuture<T>, R> doneCb) {
        this.ctx = ctx;
        this.fut = fut;
        this.doneCb = doneCb;
    }

    /** {@inheritDoc} */
    @Override public void apply(GridFuture<T> t) {
        try {
            fut.onDone(doneCb.apply(t));
        }
        catch (GridClosureException e) {
            fut.onDone(e.unwrap());
        }
        catch (RuntimeException | Error e) {
            U.warn(null, "Failed to notify chained future (is grid stopped?) [grid=" + ctx.gridName() +
                ", doneCb=" + doneCb + ", err=" + e.getMessage() + ']');

            fut.onDone(e);

            throw e;
        }
    }
}
