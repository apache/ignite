// @java.file.header

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

import java.io.*;
import java.util.concurrent.*;

/**
 * Grid-aware adapter for {@link Callable} implementations. It makes the callable object
 * {@link Serializable} and also adds peer deployment hooks to make sure that
 * deployment information is not lost.
 * <p>
 * Note that this class implements {@link GridComputeJob} interface for convenience and can be
 * used in {@link GridComputeTask} implementations directly, if needed, as an alternative to
 * {@link GridComputeJobAdapter}.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridCallable<V> extends GridLambdaAdapter implements Callable<V>, GridComputeJob {
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
     * Delegates to {@link #call()} method.
     * <p>
     * {@inheritDoc}
     *
     * @return {@inheritDoc}
     * @throws GridException {@inheritDoc}
     */
    @Override public final Object execute() throws GridException {
        try {
            return call();
        }
        catch (Throwable e) {
            throw U.cast(e);
        }
    }
}
