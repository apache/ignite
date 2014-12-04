/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.tests.p2p;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test task for {@code GridP2PContinuousDeploymentSelfTest}.
 */
public class GridP2PContinuousDeploymentTask2 extends GridComputeTaskSplitAdapter<Object, Object> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
        return Collections.singleton(new GridComputeJobAdapter() {
            @Override public Object execute() {
                X.println(">>> Executing GridP2PContinuousDeploymentTask2 job.");

                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }
}
