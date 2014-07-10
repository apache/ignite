/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.misc.client.router;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;

import java.util.*;

/**
 * Test task that simply counts number of nodes it was running on.
 * This task will produce as many grid jobs as there are nodes in the grid.
 * Each produced job will yield an output with the given message.
 */
public class RouterExampleTask extends GridComputeTaskSplitAdapter<String, Integer> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends GridComputeJob> split(int gridSize, final String arg)
        throws GridException {
        Collection<GridComputeJob> res = new ArrayList<>(gridSize);

        for (int i = 0; i < gridSize; i++) {
            res.add(new GridComputeJobAdapter() {
                @Override public Integer execute() {
                    System.out.println(">>> Executing job.");
                    System.out.println(">>> Job argument is: " + arg);

                    return 1;
                }
            });
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
        int sum = 0;

        for (GridComputeJobResult res : results)
            sum += res.<Integer>getData();

        return sum;
    }
}
