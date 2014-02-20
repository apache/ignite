// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute;

import org.gridgain.grid.*;

import java.util.*;

/**
 * Adapter for {@link GridComputeTaskAdapter}
 * overriding {@code reduce(...)} method to return {@code null}. This adapter should
 * be used for tasks that don't return any value.
 *
 * @author @java.author
 * @version @java.version
 * @param <T> Type of the task argument.
 */
public abstract class GridComputeTaskNoReduceAdapter<T> extends GridComputeTaskAdapter<T, Void> {
    /** Empty constructor. */
    protected GridComputeTaskNoReduceAdapter() {
        // No-op.
    }

    /**
     * Constructor that receives deployment information for task.
     *
     * @param p Deployment information.
     */
    protected GridComputeTaskNoReduceAdapter(GridPeerDeployAware p) {
        super(p);
    }

    /** {@inheritDoc} */
    @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }
}
