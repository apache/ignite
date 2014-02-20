// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.multispi;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * This class defines grid task for this example. Grid task is responsible for
 * splitting the task into jobs. This particular implementation creates single job
 * that later should be executed on node from segment "B" according to the topology
 * assigned to the task.
 * <p>
 * This task explicitly specifies that it should use Topology SPI named
 * {@code 'topologyA'} via {@link org.gridgain.grid.compute.GridComputeTaskSpis} annotation attached to
 * the task class definition.
 *
 * @author @java.author
 * @version @java.version
 */
@GridComputeTaskSpis(topologySpi="topologyB")
public class GridSegmentBTask extends GridComputeTaskSplitAdapter<String, Integer> {
    /**
     * Creates single job that should be executed on node from segment "B".
     *
     * @param gridSize Number of nodes in the grid.
     * @param arg Any string.
     * @return Created grid jobs for remote execution.
     * @throws GridException If split failed.
     */
    @Override public Collection<? extends GridComputeJob> split(int gridSize, String arg) throws GridException {
        return Collections.singletonList(new GridComputeJobAdapter() {
            /** Injected grid instance. */
            @GridInstanceResource
            private Grid grid;

            /*
             * Simply checks that node where job is being executed is from segment "B"
             * and prints message that node is really from expected segment.
             */
            @Nullable
            @Override public Serializable execute() throws GridException {
                assert grid != null;

                String segVal = (String)grid.localNode().attribute("segment");

                if (segVal == null || !"B".equals(segVal))
                    throw new GridException("Wrong node \"segment\" attribute value. Expected \"B\" got " + segVal);

                System.out.println(">>>");
                System.out.println(">>> Executing job on node that is from segment B.");
                System.out.println(">>>");

                return null;
            }
        });
    }

    /**
     * Ignores job results.
     *
     * @param results Job results.
     * @return {@code null}.
     */
    @Nullable
    @Override public Integer reduce(List<GridComputeJobResult> results) {
        return null;
    }
}
