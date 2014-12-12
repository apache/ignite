/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.dsi;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

/**
 *
 */
public class GridDsiRequestTask extends ComputeTaskSplitAdapter<GridDsiMessage, T3<Long, Integer, Integer>> {
    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int arg0, GridDsiMessage msg) throws IgniteCheckedException {
        return Collections.singletonList(new GridDsiPerfJob(msg));
    }

    /** {@inheritDoc} */
    @Override public T3<Long, Integer, Integer> reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        assert results.size() == 1;

        return results.get(0).getData();
    }
}
