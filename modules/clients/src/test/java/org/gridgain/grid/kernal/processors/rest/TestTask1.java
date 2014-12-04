/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 *
 */
class TestTask1 extends GridComputeTaskSplitAdapter<String, String> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends GridComputeJob> split(int gridSize, String arg) throws GridException {
        Collection<GridComputeJob> jobs = new ArrayList<>(gridSize);

        for (int i = 0; i < gridSize; i++)
            jobs.add(new GridComputeJobAdapter() {
                @Nullable
                @Override public Object execute() {
                    X.println("Test task1.");

                    return null;
                }
            });

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public String reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }
}
