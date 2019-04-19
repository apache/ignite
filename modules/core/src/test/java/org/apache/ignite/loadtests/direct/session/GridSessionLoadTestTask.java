/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.loadtests.direct.session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;

/**
 * Session load test task.
 */
public class GridSessionLoadTestTask extends ComputeTaskAdapter<Integer, Boolean> {
    /** */
    @TaskSessionResource
    private ComputeTaskSession taskSes;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** */
    private Map<String, Integer> params;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Integer arg) {
        assert taskSes != null;
        assert arg != null;
        assert arg > 0;

        Map<GridSessionLoadTestJob, ClusterNode> map = new HashMap<>(subgrid.size());

        Iterator<ClusterNode> iter = subgrid.iterator();

        Random rnd = new Random();

        params = new HashMap<>(arg);

        Collection<UUID> assigned = new ArrayList<>(subgrid.size());

        for (int i = 0; i < arg; i++) {
            // Recycle iterator.
            if (!iter.hasNext())
                iter = subgrid.iterator();

            String paramName = UUID.randomUUID().toString();

            int paramVal = rnd.nextInt();

            taskSes.setAttribute(paramName, paramVal);

            ClusterNode node = iter.next();

            assigned.add(node.id());

            map.put(new GridSessionLoadTestJob(paramName), node);

            params.put(paramName, paramVal);

            if (log.isDebugEnabled())
                log.debug("Set session attribute [name=" + paramName + ", value=" + paramVal + ']');
        }

        taskSes.setAttribute("nodes", assigned);

        return map;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public Boolean reduce(List<ComputeJobResult> results) {
        assert taskSes != null;
        assert results != null;
        assert params != null;
        assert !params.isEmpty();
        assert results.size() == params.size();

        Map<String, Integer> receivedParams = new HashMap<>();

        boolean allAttrReceived = false;

        int cnt = 0;

        while (!allAttrReceived && cnt++ < 3) {
            allAttrReceived = true;

            for (Map.Entry<String, Integer> entry : params.entrySet()) {
                assert taskSes.getAttribute(entry.getKey()) != null;

                Integer newVal = (Integer)taskSes.getAttribute(entry.getKey());

                assert newVal != null;

                receivedParams.put(entry.getKey(), newVal);

                if (newVal != entry.getValue() + 1)
                    allAttrReceived = false;
            }

            if (!allAttrReceived) {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    throw new IgniteException("Thread interrupted.", e);
                }
            }
        }

        if (log.isDebugEnabled()) {
            for (Map.Entry<String, Integer> entry : receivedParams.entrySet()) {
                log.debug("Received session attr value [name=" + entry.getKey() + ", val=" + entry.getValue()
                    + ", expected=" + (params.get(entry.getKey()) + 1) + ']');
            }
        }

        return allAttrReceived;
    }
}