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

import java.util.Arrays;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.ml.genetic.parameter.GAGridConstants;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.transactions.Transaction;

/**
 * Responsible for performing 'crossover' genetic operation for 2 X 'parent' chromosomes.
 *
 * <p>
 *
 * It relies on the GAConfiguration.getCrossOverRate() to determine probability rate of crossover for pair of
 * chromosome.
 *
 * <br/>
 *
 * CrossOverJob will randomly pick a start index j in Chromosome.getGenes[] and continue
 *
 * swapping until end of genes[] array.
 *
 * </p>
 */
public class CrossOverJob extends ComputeJobAdapter {
    /** Ignite resource */
    @IgniteInstanceResource
    private Ignite ignite = null;

    /** Ignite logger */
    @LoggerResource
    private IgniteLogger log = null;

    /** primary key of 1st chromosome */
    private Long key1;

    /** primary key of 2nd chromosome */
    private Long key2;

    /** Cross over rate */
    private double crossOverRate;

    /**
     * @param key1 Primary key for 1st chromosome
     * @param key2 Primary key for 2nd chromosome
     * @param crossOverRate CrossOver rate
     */
    public CrossOverJob(Long key1, Long key2, double crossOverRate) {
        this.key1 = key1;
        this.key2 = key2;
        this.crossOverRate = crossOverRate;
    }

    /**
     * helper routine to assist cross over
     *
     * @param newKeySwapArrForChrome New gene keys to copy starting at updateIdx
     * @param updateIdx Update Index
     * @param genekeys Original gene Keys for a chromosome
     * @return New Gene keys
     */
    private long[] crossOver(long[] newKeySwapArrForChrome, int updateIdx, long[] genekeys) {
        long[] newGeneKeys = genekeys.clone();

        int k = 0;
        for (int x = updateIdx; x < newGeneKeys.length; x++) {
            newGeneKeys[x] = newKeySwapArrForChrome[k];
            k = k + 1;
        }
        return newGeneKeys;
    }

    /**
     * Perform crossover operation
     */
    public Object execute() throws IgniteException {

        if (this.crossOverRate > Math.random()) {

            IgniteCache<Long, Chromosome> populationCache = ignite.cache(GAGridConstants.POPULATION_CACHE);

            Transaction tx = ignite.transactions().txStart();

            Chromosome chromosome1 = populationCache.localPeek(this.key1);
            Chromosome chromosome2 = populationCache.localPeek(this.key2);

            long[] genesforChrom1 = chromosome1.getGenes();
            long[] genesforChrom2 = chromosome2.getGenes();

            Random rn = new Random();

            // compute index to start for copying respective genes
            int geneIdxStartSwap = rn.nextInt(genesforChrom1.length);

            long[] newKeySwapArrForChrome1 =
                Arrays.copyOfRange(genesforChrom2, geneIdxStartSwap, genesforChrom1.length);
            long[] newKeySwapArrForChrome2 =
                Arrays.copyOfRange(genesforChrom1, geneIdxStartSwap, genesforChrom1.length);

            long[] newGeneKeysForChrom1 = crossOver(newKeySwapArrForChrome1, geneIdxStartSwap, genesforChrom1);
            long[] newGeneKeysForChrom2 = crossOver(newKeySwapArrForChrome2, geneIdxStartSwap, genesforChrom2);

            chromosome1.setGenes(newGeneKeysForChrom1);
            populationCache.put(chromosome1.id(), chromosome1);

            chromosome2.setGenes(newGeneKeysForChrom2);
            populationCache.put(chromosome2.id(), chromosome2);

            tx.commit();

        }

        return null;
    }

}
