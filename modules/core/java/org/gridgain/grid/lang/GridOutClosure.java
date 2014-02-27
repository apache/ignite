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

import java.util.concurrent.*;

/**
 * Defines a convenient {@code side-effect only} factory closure. This closure takes no parameters
 * and returns instance of given type every time its {@link #apply()} method is called. Most
 * implementations will return a new instance every time, however, there's no requirement for that.
 * Note also that factory closure doesn't have free variables (i.e. it has {@code void} as its
 * fre variable).
 * <h2 class="header">Thread Safety</h2>
 * Note that this interface does not impose or assume any specific thread-safety by its
 * implementations. Each implementation can elect what type of thread-safety it provides,
 * if any.
 * <p>
 * Note that this class implements {@link GridComputeJob} interface for convenience and can be
 * used in {@link GridComputeTask} implementations directly, if needed, as an alternative to
 * {@link GridComputeJobAdapter}.
 *
 * @author @java.author
 * @version @java.version
 * @param <T> Type of return value from this closure.
 * @see GridFunc
 */
public abstract class GridOutClosure<T> extends GridLambdaAdapter implements Callable<T>, GridComputeJob {
    /**
     * Factory closure body.
     *
     * @return Element.
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
