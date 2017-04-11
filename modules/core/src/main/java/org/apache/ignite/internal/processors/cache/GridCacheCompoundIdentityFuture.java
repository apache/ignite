package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridCacheCompoundIdentityFuture<T> extends GridCompoundIdentityFuture<T> implements GridCacheFuture<T> {
    /** Future start time. */
    private final long startTime = U.currentTimeMillis();

    /** Future end time. */
    private volatile long endTime;

    /**
     * @param rdc Reducer.
     */
    protected GridCacheCompoundIdentityFuture(@Nullable IgniteReducer<T, T> rdc) {
        super(rdc);
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public long duration() {
        long endTime = this.endTime;

        return (endTime == 0 ? U.currentTimeMillis() : endTime) - startTime;
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable T res, @Nullable Throwable err, boolean cancel) {
        if(super.onDone(res, err, cancel)){
            endTime = U.currentTimeMillis();
            return true;
        }

        return false;
    }
}
