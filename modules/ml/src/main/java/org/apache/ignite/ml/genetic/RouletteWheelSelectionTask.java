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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeLoadBalancer;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.ml.genetic.parameter.GAConfiguration;
import org.apache.ignite.ml.genetic.parameter.GAGridConstants;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoadBalancerResource;
import org.jetbrains.annotations.NotNull;

/**
 * Responsible for performing Roulette Wheel selection.
 */
public class RouletteWheelSelectionTask extends ComputeTaskAdapter<LinkedHashMap<Long, Double>, List<Long>> {
    /** Ignite resource. */
    @IgniteInstanceResource
    private Ignite ignite;

    // Inject load balancer.
    @LoadBalancerResource
    ComputeLoadBalancer balancer;

    /** GAConfiguration */
    private GAConfiguration cfg;

    /**
     * @param cfg GAConfiguration
     */
    public RouletteWheelSelectionTask(GAConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Calculate total fitness of population
     *
     * @return Double value representing total fitness score of population
     */
    private Double calculateTotalFitness() {
        double totalFitnessScore = 0;

        IgniteCache<Long, Chromosome> cache = ignite.cache(GAGridConstants.POPULATION_CACHE);

        SqlFieldsQuery sql = new SqlFieldsQuery("select SUM(FITNESSSCORE) from Chromosome");

        // Iterate over the result set.
        try (QueryCursor<List<?>> cursor = cache.query(sql)) {
            for (List<?> row : cursor)
                totalFitnessScore = (Double)row.get(0);
        }

        return totalFitnessScore;
    }

    /**
     * @param nodes List of ClusterNode.
     * @param chromosomeKeyFitness Map of key/fitness score pairs.
     * @return Map of nodes to jobs.
     */
    @NotNull @Override public Map<ComputeJob, ClusterNode> map(List<ClusterNode> nodes,
        LinkedHashMap<Long, Double> chromosomeKeyFitness) throws IgniteException {
        Map<ComputeJob, ClusterNode> map = new HashMap<>();

        Affinity affinity = ignite.affinity(GAGridConstants.POPULATION_CACHE);
        Double totalFitness = this.calculateTotalFitness();

        int populationSize = this.cfg.getPopulationSize();

        for (int i = 0; i < populationSize; i++) {
            // Pick the next best balanced node for the job.
            RouletteWheelSelectionJob job = new RouletteWheelSelectionJob(totalFitness, chromosomeKeyFitness);
            map.put(job, balancer.getBalancedNode(job, null));
        }

        return map;
    }

    /**
     * Return list of parent Chromosomes.
     *
     * @param list List of ComputeJobResult.
     * @return List of Chromosome keys.
     */
    @Override public List<Long> reduce(List<ComputeJobResult> list) throws IgniteException {
        List<Chromosome> parents = list.stream().map((x) -> (Chromosome)x.getData()).collect(Collectors.toList());

        return createParents(parents);
    }

    /**
     * Create new parents and add to populationCache
     *
     * @param parents Chromosomes chosen to breed
     * @return List of Chromosome keys.
     */
    private List<Long> createParents(List<Chromosome> parents) {
        IgniteCache<Long, Chromosome> cache = ignite.cache(GAGridConstants.POPULATION_CACHE);
        cache.clear();

        List<Long> keys = new ArrayList();

        parents.stream().forEach((x) -> {
            long[] genes = x.getGenes();
            Chromosome newparent = new Chromosome(genes);
            cache.put(newparent.id(), newparent);
            keys.add(newparent.id());
        });

        return keys;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        IgniteException err = res.getException();

        if (err != null)
            return ComputeJobResultPolicy.FAILOVER;

        // If there is no exception, wait for all job results.
        return ComputeJobResultPolicy.WAIT;
    }
}
