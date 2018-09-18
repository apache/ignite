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
import java.util.Random;
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
import org.apache.ignite.ml.genetic.parameter.GAConfiguration;
import org.apache.ignite.ml.genetic.parameter.GAGridConstants;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Responsible for applying mutation on respective chromosomes.  <br/>
 *
 * MutateTask leverages Ignite's data affinity capabilities for routing MutateJobs to primary IgniteNode where <br/>
 * chromosomes reside.<br/>
 */
public class MutateTask extends ComputeTaskAdapter<List<Long>, Boolean> {
    /** Ignite instance */
    @IgniteInstanceResource
    private Ignite ignite = null;

    /** GAConfiguration */
    private GAConfiguration cfg;

    /**
     * @param cfg GAConfiguration
     */
    public MutateTask(GAConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * choose mutated genes.
     *
     * @return Gene primary keys
     */
    private List<Long> getMutatedGenes() {
        List<Long> mutatedGenes = new ArrayList<Long>();
        cfg.getChromosomeLen();

        for (int i = 0; i < cfg.getChromosomeLen(); i++)
            mutatedGenes.add(selectGene(i));

        return mutatedGenes;
    }

    /**
     * @param nodes List of ClusterNode
     * @param chromosomeKeys Primary keys for respective chromosomes
     */
    public Map map(List<ClusterNode> nodes, List<Long> chromosomeKeys) throws IgniteException {

        Map<ComputeJob, ClusterNode> map = new HashMap<>();
        Affinity affinity = ignite.affinity(GAGridConstants.POPULATION_CACHE);

        for (Long key : chromosomeKeys) {
            MutateJob ajob = new MutateJob(key, getMutatedGenes(), this.cfg.getMutationRate());
            ClusterNode primary = affinity.mapKeyToNode(key);
            map.put(ajob, primary);
        }
        return map;
    }

    /**
     * We return TRUE if success, else Exection is thrown.
     *
     * @param list List of ComputeJobResult
     * @return Boolean value
     */
    public Boolean reduce(List<ComputeJobResult> list) throws IgniteException {
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
     * select a gene from the Gene pool
     *
     * @return Primary key of Gene
     */
    private long selectAnyGene() {
        int idx = selectRandomIndex(cfg.getGenePool().size());
        Gene gene = cfg.getGenePool().get(idx);
        return gene.id();
    }

    /**
     * select a gene based from the Gene pool
     *
     * @param k Gene index in Chromosome.
     * @return Primary key of Gene
     */
    private long selectGene(int k) {
        if (cfg.getChromosomeCriteria() == null)
            return (selectAnyGene());
        else
            return (selectGeneByChromsomeCriteria(k));
    }

    /**
     * method assumes ChromosomeCriteria is set.
     *
     * @param k Gene index in Chromosome.
     * @return Primary key of Gene
     */
    private long selectGeneByChromsomeCriteria(int k) {
        List<Gene> genes = new ArrayList<Gene>();

        StringBuffer sbSqlClause = new StringBuffer("_val like '");
        sbSqlClause.append("%");
        sbSqlClause.append(cfg.getChromosomeCriteria().getCriteria().get(k));
        sbSqlClause.append("%'");

        IgniteCache<Long, Gene> cache = ignite.cache(GAGridConstants.GENE_CACHE);

        SqlQuery sql = new SqlQuery(Gene.class, sbSqlClause.toString());

        try (QueryCursor<Entry<Long, Gene>> cursor = cache.query(sql)) {
            for (Entry<Long, Gene> e : cursor)
                genes.add(e.getValue());
        }

        int idx = selectRandomIndex(genes.size());

        Gene gene = genes.get(idx);
        return gene.id();
    }

    /**
     * select an index at random
     *
     * @param sizeOfGenePool Size of gene pool
     * @return Index of Gene to be selected
     */
    private int selectRandomIndex(int sizeOfGenePool) {
        Random randomGenerator = new Random();
        return randomGenerator.nextInt(sizeOfGenePool);
    }

}
