/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.tests.p2p;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test task for {@code GridCacheDeploymentSelfTest}.
 */
public class GridCacheDeploymentTestTask1 extends GridComputeTaskAdapter<GridNode, Object> {
    /** Number of puts. */
    private static final int PUT_CNT = 100;

    /** {@inheritDoc} */
    @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid,
        @Nullable GridNode node) throws GridException {
        return F.asMap(
            new GridComputeJobAdapter() {
                @GridInstanceResource
                private Ignite ignite;

                @Override public Object execute() throws GridException {
                    X.println("Executing GridCacheDeploymentTestTask1 job on node " +
                        ignite.cluster().localNode().id());

                    GridCache<String, GridCacheDeploymentTestValue> cache = ignite.cache(null);

                    for (int i = 0; i < PUT_CNT; i++)
                        cache.putx("1" + i, new GridCacheDeploymentTestValue());

                    return null;
                }
            },
            node
        );
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }
}
