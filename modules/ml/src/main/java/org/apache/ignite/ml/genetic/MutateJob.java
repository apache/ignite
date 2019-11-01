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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.ml.genetic.parameter.GAGridConstants;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;

/**
 * Responsible for applying mutation on respective Chromosome based on mutation Rate
 */
public class MutateJob extends ComputeJobAdapter {
    /** primary key of Chromosome to mutate **/
    private Long key;

    /** primary keys of genes to be used in mutation **/
    private List<Long> mutatedGeneKeys;

    /** Ignite instance */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Mutation Rate **/
    private double mutationRate;

    /**
     * @param key Primary key of chromosome
     * @param mutatedGeneKeys Primary keys of genes to be used in mutation
     * @param mutationRate Mutation rate
     */
    public MutateJob(Long key, List<Long> mutatedGeneKeys, double mutationRate) {
        this.key = key;
        this.mutationRate = mutationRate;
        this.mutatedGeneKeys = mutatedGeneKeys;
    }

    /** {@inheritDoc} */
    @Override public Boolean execute() throws IgniteException {

        IgniteCache<Long, Chromosome> populationCache = ignite.cache(GAGridConstants.POPULATION_CACHE);

        Chromosome chromosome = populationCache.localPeek(key);

        long[] geneKeys = chromosome.getGenes();

        for (int k = 0; k < this.mutatedGeneKeys.size(); k++) {
            // Mutate gene based on MutatonRate
            if (this.mutationRate > Math.random())
                geneKeys[k] = this.mutatedGeneKeys.get(k);
        }

        chromosome.setGenes(geneKeys);

        Transaction tx = ignite.transactions().txStart();

        populationCache.put(chromosome.id(), chromosome);

        tx.commit();

        return Boolean.TRUE;
    }

}
