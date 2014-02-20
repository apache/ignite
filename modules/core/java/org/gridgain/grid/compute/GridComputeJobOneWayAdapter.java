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
 * Utility adapter for jobs returning no value.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridComputeJobOneWayAdapter extends GridComputeJobAdapter {
    /** {@inheritDoc} */
    @Override public final Object execute() throws GridException {
        oneWay();

        return null;
    }

    /**
     * This method is called by {@link #execute()} and allows to avoid manual {@code return null}.
     *
     * @throws GridException If job execution caused an exception. This exception will be
     *      returned in {@link GridComputeJobResult#getException()} method passed into
     *      {@link GridComputeTask#result(GridComputeJobResult, List)} method into task on caller node.
     *      If execution produces a {@link RuntimeException} or {@link Error}, then
     *      it will be wrapped into {@link GridException}.
     */
    protected abstract void oneWay() throws GridException;
}
