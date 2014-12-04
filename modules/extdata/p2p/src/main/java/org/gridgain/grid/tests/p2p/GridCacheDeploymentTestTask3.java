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
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test task for {@code GridCacheDeploymentSelfTest}.
 */
public class GridCacheDeploymentTestTask3 extends ComputeTaskAdapter<T2<ClusterNode, String>, Object> {
    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable T2<ClusterNode, String> tup) throws GridException {
        final String val = tup.getValue();

        return F.asMap(
                new ComputeJobAdapter() {
                    @IgniteInstanceResource
                    private Ignite ignite;

                    @Override public Object execute() throws GridException {
                        X.println("Executing GridCacheDeploymentTestTask3 job on node " +
                                ignite.cluster().localNode().id());

                        ignite.<String, GridCacheDeploymentTestValue>cache(null).putx(val,
                                new GridCacheDeploymentTestValue());

                        return null;
                    }
                },
                tup.get1()
        );
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
        return null;
    }
}
