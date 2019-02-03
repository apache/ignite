/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.genetic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache.Entry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.ml.genetic.parameter.GAGridConstants;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Responsible for performing truncate selection.
 */
public class TruncateSelectionTask extends ComputeTaskAdapter<List<Long>, Boolean> {
    /** Ignite resource. */
    @IgniteInstanceResource
    private Ignite ignite = null;

    /** Fittest keys. */
    private List<Long> fittestKeys;

    /** Number of Copies. */
    private int numOfCopies;

    /**
     * @param fittestKeys List of long.
     * @param numOfCopies Number of Copies.
     */
    public TruncateSelectionTask(List<Long> fittestKeys, int numOfCopies) {
        this.fittestKeys = fittestKeys;
        this.numOfCopies = numOfCopies;
    }

    /**
     * Retrieve a chromosome.
     *
     * @param key Primary key of chromosome.
     * @return Chromosome.
     */
    private Chromosome getChromosome(Long key) {
        IgniteCache<Long, Chromosome> cache = ignite.cache(GAGridConstants.POPULATION_CACHE);
        StringBuffer sbSqlClause = new StringBuffer();
        sbSqlClause.append("_key IN (");
        sbSqlClause.append(key);
        sbSqlClause.append(")");

        Chromosome chromosome = null;

        SqlQuery sql = new SqlQuery(Chromosome.class, sbSqlClause.toString());

        try (QueryCursor<Entry<Long, Chromosome>> cursor = cache.query(sql)) {
            for (Entry<Long, Chromosome> e : cursor)
                chromosome = (e.getValue());
        }

        return chromosome;
    }

    /**
     * Return a List of lists containing keys.
     *
     * @return List of lists containing keys.
     */
    private List<List<Long>> getEnhancedPopulation() {
        List<List<Long>> list = new ArrayList<List<Long>>();

        for (Long key : fittestKeys) {
            Chromosome cp = getChromosome(key);
            for (int i = 0; i < numOfCopies; i++) {
                long[] thegenes = cp.getGenes();
                List<Long> geneList = new ArrayList<Long>();
                for (int k = 0; k < cp.getGenes().length; k++)
                    geneList.add(thegenes[k]);

                list.add(geneList);
            }
        }

        return list;
    }

    /**
     * @param nodes List of ClusterNode.
     * @param chromosomeKeys Primary keys for respective chromosomes.
     * @return Map of nodes to jobs.
     */
    public Map map(List<ClusterNode> nodes, List<Long> chromosomeKeys) throws IgniteException {
        Map<ComputeJob, ClusterNode> map = new HashMap<>();
        Affinity affinity = ignite.affinity(GAGridConstants.POPULATION_CACHE);

        // Retrieve enhanced population
        List<List<Long>> enhancedPopulation = getEnhancedPopulation();

        int k = 0;
        for (Long key : chromosomeKeys) {
            TruncateSelectionJob ajob = new TruncateSelectionJob(key, (List)enhancedPopulation.get(k));
            ClusterNode primary = affinity.mapKeyToNode(key);
            map.put(ajob, primary);
            k = k + 1;
        }
        return map;
    }

    /**
     * We return TRUE if success, else Exception is thrown.
     *
     * @param list List of ComputeJobResult.
     * @return Boolean value.
     */
    public Boolean reduce(List<ComputeJobResult> list) throws IgniteException {
        return Boolean.TRUE;
    }

    /**
     * @param res ComputeJobResult.
     * @param rcvd List of ComputeJobResult.
     * @return ComputeJobResultPolicy.
     */
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        IgniteException err = res.getException();

        if (err != null)
            return ComputeJobResultPolicy.FAILOVER;

        // If there is no exception, wait for all job results.
        return ComputeJobResultPolicy.WAIT;
    }

}
