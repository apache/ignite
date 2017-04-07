package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class GridCacheFutureAdapter<R> extends GridFutureAdapter<R> implements GridCacheFuture<R> {
    /** Future start time. */
    private final long startTime = U.currentTimeMillis();

    /** Future end time. */
    private volatile long endTime;

    /**
     * Default constructor.
     */
    public GridCacheFutureAdapter() {
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public long duration() {
        long endTime = this.endTime;

        return endTime == 0 ? U.currentTimeMillis() - startTime : endTime - startTime;
    }

    /** {@inheritDoc} */
    @Override protected boolean onDone(@Nullable R res, @Nullable Throwable err, boolean cancel) {
        if(super.onDone(res, err, cancel)){
            endTime = U.currentTimeMillis();
            return true;
        }

        return false;
    }
}
