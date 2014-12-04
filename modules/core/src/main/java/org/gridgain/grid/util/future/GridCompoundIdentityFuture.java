/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.future;

import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Future composed of multiple inner futures.
 */
public class GridCompoundIdentityFuture<T> extends GridCompoundFuture<T, T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Empty constructor required for {@link Externalizable}. */
    public GridCompoundIdentityFuture() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    public GridCompoundIdentityFuture(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param ctx Context.
     * @param rdc Reducer.
     */
    public GridCompoundIdentityFuture(GridKernalContext ctx, @Nullable IgniteReducer<T, T> rdc) {
        super(ctx, rdc);
    }

    /**
     * @param ctx Context.
     * @param rdc  Reducer to add.
     * @param futs Futures to add.
     */
    public GridCompoundIdentityFuture(GridKernalContext ctx, @Nullable IgniteReducer<T, T> rdc,
        @Nullable Iterable<IgniteFuture<T>> futs) {
        super(ctx, rdc, futs);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCompoundIdentityFuture.class, this, super.toString());
    }
}
