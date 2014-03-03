/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.concurrent.*;

/**
 * Closure that does not take any parameters and returns a value.
 *
 * @param <T> Type of return value from this closure.
 */
public abstract class GridOutClosure<T> extends GridLambdaAdapter implements Callable<T>, GridComputeJob {
    /**
     * Closure body.
     *
     * @return Return value.
     */
    public abstract T apply();

    /**
     * Delegates to {@link #apply()} method.
     * <p>
     * {@inheritDoc}
     */
    @Override public final T call() {
        return apply();
    }

    /**
     * Does nothing by default. Child classes may override this method
     * to provide implementation-specific cancellation logic.
     * <p>
     * Note that this method is here only to support {@link GridComputeJob} interface
     * and only makes sense whenever this class is used as grid job or is
     * executed via any of {@link GridProjection} methods.
     * <p>
     * {@inheritDoc}
     */
    @Override public void cancel() {
        // No-op.
    }

    /**
     * Delegates to {@link #apply()} method.
     * <p>
     * {@inheritDoc}
     *
     * @return {@inheritDoc}
     * @throws GridException {@inheritDoc}
     */
    @Override public final Object execute() throws GridException {
        try {
            return apply();
        }
        catch (Throwable e) {
            throw U.cast(e);
        }
    }
}
