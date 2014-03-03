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

import java.io.*;

/**
 * Grid-aware adapter for {@link Runnable} implementations. It adds {@link Serializable} interface
 * to {@link Runnable} object. Use this class for executing distributed computations on the grid,
 * like in {@link GridCompute#run(Runnable)} method.
 */
public abstract class GridRunnable extends GridLambdaAdapter implements Runnable, GridComputeJob {
    /** {@inheritDoc} */
    @Override public void cancel() {
        // No-op.
    }

    /**
     * Delegates to {@link #run()} method.
     * <p>
     * {@inheritDoc}
     *
     * @return {@inheritDoc}
     * @throws GridException {@inheritDoc}
     */
    @Override public final Object execute() throws GridException {
        try {
            run();
        }
        catch (Throwable e) {
            throw U.cast(e);
        }

        return null;
    }
}
