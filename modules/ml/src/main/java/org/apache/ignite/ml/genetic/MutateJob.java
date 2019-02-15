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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;

import org.apache.ignite.ml.genetic.parameter.GAGridConstants;

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
    private Ignite ignite = null;

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

    /**
     * Perform mutation
     *
     * @return Boolean value
     */
    public Boolean execute() throws IgniteException {

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
