/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.compute.gridify.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;

import java.util.*;

/**
 * P2P test task.
 */
public class GridP2PTestTask extends ComputeTaskAdapter<Object, Integer> {
    /** */
    public static final String TASK_NAME = GridP2PTestTask.class.getName();

    /** */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) throws IgniteCheckedException {
        assert subgrid != null;
        assert !subgrid.isEmpty();

        Integer arg1 = null;

        if (arg instanceof GridifyArgument)
            arg1 = (Integer)((GridifyArgument)arg).getMethodParameters()[0];
        else if (arg instanceof Integer)
            arg1 = (Integer)arg;
        else
            assert false : "Failed to map task (unknown argument type) [type=" + arg.getClass() + ", val=" + arg + ']';

        Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());

        for (ClusterNode node : subgrid)
            if (!node.id().equals(ignite.configuration().getNodeId()))
                map.put(new GridP2PTestJob(arg1), node);

        return map;
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        assert results.size() == 1 : "Results [received=" + results.size() + ", expected=" + 1 + ']';

        ComputeJobResult res = results.get(0);

        if (log.isInfoEnabled())
            log.info("Got job result for aggregation: " + res);

        if (res.getException() != null)
            throw res.getException();

        return res.getData();
    }
}
