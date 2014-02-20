// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.deployment.direct;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Example task used to demonstrate direct task deployment through API.
 * For this example this task as available on the classpath, however
 * in real life that may not always be the case. In those cases
 * you should use explicit {@link Grid#deployTask(Class)} apply and
 * then use {@link Grid#execute(String, Object, org.gridgain.grid.lang.GridPredicate[])}
 * method passing your task name as first parameter.
 * <p>
 * Note that this task specifies explicit task name. Task name is optional
 * and is added here for demonstration purpose. If not provided, it will
 * default to the task class name.
 *
 * @author @java.author
 * @version @java.version
 */
@GridComputeTaskName(GridDeploymentExample.TASK_NAME)
public class GridDeploymentExampleTask extends GridComputeTaskSplitAdapter<String, Object> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends GridComputeJob> split(int gridSize, String arg) throws GridException {
        Collection<GridComputeJob> jobs = new ArrayList<>(gridSize);

        for (int i = 0; i < gridSize; i++) {
            jobs.add(new GridComputeJobAdapter() {
                @Nullable
                @Override public Serializable execute() {
                    System.out.println(">>> Executing deployment example job on this node.");

                    // This job does not return any result.
                    return null;
                }
            });
        }

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }
}
