/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.tests.p2p;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test task for {@code GridCacheDeploymentSelfTest}.
 */
public class GridCacheDeploymentTestTask2 extends GridComputeTaskAdapter<ClusterNode, Object> {
    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable ClusterNode node) throws GridException {
        return F.asMap(
            new GridComputeJobAdapter() {
                @GridInstanceResource
                private Ignite ignite;

                @Override public Object execute() {
                    X.println("Executing GridCacheDeploymentTestTask2 job on node " +
                        ignite.cluster().localNode().id());

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
