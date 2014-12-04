/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Future that delegates to some other future.
 */
public class GridNioEmbeddedFuture<R> extends GridNioFutureImpl<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Callback to notify that future is finished.
     * This method must delegate to {@link #onDone(GridNioFuture, Throwable)} method.
     *
     * @param res Result.
     */
    public final void onDone(GridNioFuture<R> res) {
        onDone(res, null);
    }

    /**
     * Callback to notify that future is finished. Note that if non-{@code null} exception is passed in
     * the result value will be ignored.
     *
     * @param delegate Optional result.
     * @param err Optional error.
     */
    public void onDone(@Nullable GridNioFuture<R> delegate, @Nullable Throwable err) {
        assert delegate != null || err != null;

        if (err != null)
            onDone(err);
        else delegate.listenAsync(new IgniteInClosure<GridNioFuture<R>>() {
            @Override public void apply(GridNioFuture<R> t) {
                try {
                    onDone(t.get());
                }
                catch (IOException | GridException e) {
                    onDone(e);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioEmbeddedFuture.class, this, super.toString());
    }
}
