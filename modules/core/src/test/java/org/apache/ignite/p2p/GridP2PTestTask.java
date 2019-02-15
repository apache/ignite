/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.p2p;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.gridify.GridifyArgument;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * P2P test task.
 */
public class GridP2PTestTask extends ComputeTaskAdapter<Object, Integer> {
    /** */
    public static final String TASK_NAME = GridP2PTestTask.class.getName();

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
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

        UUID nodeId = ignite != null ? ignite.configuration().getNodeId() : null;

        for (ClusterNode node : subgrid)
            if (!node.id().equals(nodeId))
                map.put(new GridP2PTestJob(arg1), node);

        return map;
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<ComputeJobResult> results) {
        assert results.size() == 1 : "Results [received=" + results.size() + ", expected=" + 1 + ']';

        ComputeJobResult res = results.get(0);

        if (log.isInfoEnabled())
            log.info("Got job result for aggregation: " + res);

        if (res.getException() != null)
            throw res.getException();

        return res.getData();
    }
}