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

package org.apache.ignite.examples.ml.genetic.knapsack;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.ml.genetic.Chromosome;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.parameter.ITerminateCriteria;
import org.apache.ignite.ml.genetic.utils.GAGridUtils;

/**
 * Represents the terminate condition for {@link KnapsackGAExample}.
 * <p>
 * Class terminates Genetic algorithm when once GA Grid has performed 30 generations.</p>
 */
public class KnapsackTerminateCriteria implements ITerminateCriteria {
    /** Ignite instance. */
    private Ignite ignite;

    /** Ignite logger. */
    private IgniteLogger igniteLog;

    /**
     * Create class instance.
     *
     * @param ignite Ignite instance.
     */
    public KnapsackTerminateCriteria(Ignite ignite) {
        this.ignite = ignite;
        this.igniteLog = this.ignite.log();
    }

    /**
     * Check whether termination condition is met.
     *
     * @param fittestChromosome Most fit chromosome at for the nth generation.
     * @param averageFitnessScore Average fitness score as of the nth generation.
     * @param currGeneration Current generation.
     * @return Status whether condition is met or not.
     */
    public boolean isTerminationConditionMet(Chromosome fittestChromosome, double averageFitnessScore,
        int currGeneration) {
        boolean isTerminate = true;

        igniteLog.info("##########################################################################################");
        igniteLog.info("Generation: " + currGeneration);
        igniteLog.info("Fittest is Chromosome Key: " + fittestChromosome);
        igniteLog.info("Total value is: " + fittestChromosome.getFitnessScore());
        igniteLog.info("Total weight is: " + calculateTotalWeight(GAGridUtils.getGenesInOrderForChromosome(ignite, fittestChromosome)));
        igniteLog.info("Avg Chromosome Fitness: " + averageFitnessScore);
        igniteLog.info("Chromosome: " + fittestChromosome);
        printItems(GAGridUtils.getGenesInOrderForChromosome(ignite, fittestChromosome));
        igniteLog.info("##########################################################################################");

        if (!(currGeneration > 29))
            isTerminate = false;

        return isTerminate;
    }

    /**
     * Calculate total weight.
     *
     * @param genes List of Genes.
     * @return Calculated value.
     */
    private double calculateTotalWeight(List<Gene> genes) {
        double totalWeight = 0;
        for (Gene gene : genes)
            totalWeight = totalWeight + ((Item)gene.getVal()).getWeight();

        return totalWeight;
    }

    /**
     * Helper to print items in knapsack.
     *
     * @param genes List of Genes.
     */
    private void printItems(List<Gene> genes) {
        for (Gene gene : genes) {
            igniteLog.info("------------------------------------------------------------------------------------------");
            igniteLog.info("Name: " + ((Item)gene.getVal()).getName());
            igniteLog.info("Weight: " + ((Item)gene.getVal()).getWeight());
            igniteLog.info("Value: " + ((Item)gene.getVal()).getVal());
        }
    }
}
