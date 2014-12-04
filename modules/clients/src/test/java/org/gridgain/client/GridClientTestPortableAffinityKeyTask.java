/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.portables.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Task used to test portable affinity key.
 */
public class GridClientTestPortableAffinityKeyTask extends ComputeTaskAdapter<Object, Boolean> {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> clusterNodes,
        @Nullable final Object arg) throws GridException {
        for (ClusterNode node : clusterNodes) {
            if (node.isLocal())
                return Collections.singletonMap(new ComputeJobAdapter() {
                    @Override public Object execute() throws GridException {
                        return executeJob(arg);
                    }
                }, node);
        }

        throw new GridException("Failed to find local node in task topology: " + clusterNodes);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Boolean reduce(List<ComputeJobResult> results) throws GridException {
        return results.get(0).getData();
    }

    /**
     * @param arg Argument.
     * @return Execution result.
     * @throws GridException If failed.
     */
     protected Boolean executeJob(Object arg) throws GridException {
        Collection args = (Collection)arg;

        Iterator<Object> it = args.iterator();

        assert args.size() == 3 : args.size();

        GridPortableObject obj = (GridPortableObject)it.next();

        String cacheName = (String)it.next();

        String expAffKey = (String)it.next();

        Object affKey = ignite.cache(cacheName).affinity().affinityKey(obj);

        if (!expAffKey.equals(affKey))
            throw new GridException("Unexpected affinity key: " + affKey);

        if (!ignite.cache(cacheName).affinity().mapKeyToNode(obj).isLocal())
            throw new GridException("Job is not run on primary node.");

        return true;
    }
}
