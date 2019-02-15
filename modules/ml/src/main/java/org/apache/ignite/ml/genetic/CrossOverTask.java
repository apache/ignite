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

package org.apache.ignite.ml.genetic;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.ml.genetic.parameter.GAConfiguration;
import org.apache.ignite.ml.genetic.parameter.GAGridConstants;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Responsible for assigning 2 X 'parent' chromosomes to produce 2 X 'child' chromosomes.
 *
 * <p>
 *
 * CrossOverTask leverages  Ignite's data affinity capabilities for routing CrossOverJobs to primary <br/> IgniteNode
 * where 'parent' chromosomes reside.
 *
 * </p>
 */
public class CrossOverTask extends ComputeTaskAdapter<List<Long>, Boolean> {
    /** Ignite instance */
    @IgniteInstanceResource
    private Ignite ignite = null;

    /** GAConfiguration */
    private GAConfiguration cfg;

    /**
     * @param cfg GAConfiguration
     */
    public CrossOverTask(GAConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Map Jobs to nodes using data affinity.
     *
     * @param nodes Cluster Nodes
     * @param chromosomeKeys Primary keys for respective chromosomes
     * @return A map of nodes to jobs.
     */
    public Map map(List<ClusterNode> nodes, List<Long> chromosomeKeys) throws IgniteException {

        Map<ComputeJob, ClusterNode> map = new HashMap<>();

        Affinity affinity = ignite.affinity(GAGridConstants.POPULATION_CACHE);

        Map<ClusterNode, Collection<Long>> nodeKeys = affinity.mapKeysToNodes(chromosomeKeys);

        for (Map.Entry<ClusterNode, Collection<Long>> entry : nodeKeys.entrySet()) {
            ClusterNode aNode = entry.getKey();
            map = setupCrossOver(aNode, (List<Long>)entry.getValue(), map);
        }
        return map;
    }

    /**
     * We return TRUE if success, else Exection is thrown.
     *
     * @param list ComputeJobResult
     * @return Boolean value; if operationa was successful return true, otherwise Exception
     */
    public Boolean reduce(List<ComputeJobResult> list) throws IgniteException {
        // TODO Auto-generated method stub
        return Boolean.TRUE;
    }

    /**
     * @param res ComputeJobResult
     * @param rcvd List of ComputeJobResult
     * @return ComputeJobResultPolicy
     */
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        IgniteException err = res.getException();

        if (err != null)
            return ComputeJobResultPolicy.FAILOVER;

        // If there is no exception, wait for all job results.
        return ComputeJobResultPolicy.WAIT;

    }

    /**
     * Helper method to help assign ComputeJobs to respective ClusterNodes
     *
     * @param clusterNode
     * @param keys Primary keys of Chromosomes
     * @param map Nodes where jobs will be sent
     * @return A map of ComputeJob/ClusterNode's
     */
    private Map<ComputeJob, ClusterNode> setupCrossOver(ClusterNode clusterNode, List<Long> keys,
        Map<ComputeJob, ClusterNode> map) {
        // Calculate number of Jobs = keys / 2
        // as we desire pairs of Chromosomes to be swapped
        int numOfJobs = keys.size() / 2;
        int k = 0;
        for (int i = 0; i < numOfJobs; i++) {
            Long key1 = keys.get(k);
            Long key2 = keys.get(k + 1);

            CrossOverJob job = new CrossOverJob(key1, key2, this.cfg.getCrossOverRate());
            map.put(job, clusterNode);
            k = k + 2;
        }
        return map;
    }
}
