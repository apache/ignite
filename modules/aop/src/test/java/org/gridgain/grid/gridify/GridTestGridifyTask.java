/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.gridify;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.gridify.*;

import java.util.*;

/**
 * Test gridify task.
 */
@GridComputeTaskName(GridTestGridifyTask.TASK_NAME)
public class GridTestGridifyTask extends GridComputeTaskSplitAdapter<GridifyArgument, Object> {
    /** */
    public static final String TASK_NAME = "org.gridgain.grid.gridify.GridTestGridifyTask";

    /** {@inheritDoc} */
    @Override public Collection<? extends GridComputeJob> split(int gridSize, GridifyArgument arg) throws GridException {
        assert arg.getMethodParameters().length == 1;

        return Collections.singletonList(new GridTestGridifyJob((String)arg.getMethodParameters()[0]));
    }

    /** {@inheritDoc} */
    @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
        assert results.size() == 1;

        return results.get(0).getData();
    }
}
