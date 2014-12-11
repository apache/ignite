/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;

import java.util.*;

/**
 * Test task.
 */
public class GridTestTask extends ComputeTaskSplitAdapter<Object, Object> {
    /** Logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) {
        if (log.isDebugEnabled())
            log.debug("Splitting task [task=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

        Collection<ComputeJob> refs = new ArrayList<>(gridSize);

        for (int i = 0; i < gridSize; i++)
            refs.add(new GridTestJob(arg.toString() + i + 1));

        return refs;
    }

    /** {@inheritDoc} */
    @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Reducing task [task=" + this + ", results=" + results + ']');

        return results;
    }
}
