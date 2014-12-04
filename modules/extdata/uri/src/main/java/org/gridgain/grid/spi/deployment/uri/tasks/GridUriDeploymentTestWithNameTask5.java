/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri.tasks;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;

import java.util.*;

/**
 * URI deployment test task with name.
 */
@GridComputeTaskName("GridUriDeploymentTestWithNameTask5")
public class GridUriDeploymentTestWithNameTask5 extends GridComputeTaskSplitAdapter<Object, Object> {
    /**
     * {@inheritDoc}
     */
    @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) throws GridException {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }
}
