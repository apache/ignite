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

package org.apache.ignite.examples.ml.genetic.change;

import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.genetic.Chromosome;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.parameter.ITerminateCriteria;
import org.apache.ignite.ml.genetic.utils.GAGridUtils;

/**
 * Terminate Condition implementation for {@link OptimizeMakeChangeGAExample}.
 */
public class OptimizeMakeChangeTerminateCriteria implements ITerminateCriteria {
    /** */
    private final Ignite ignite;

    /** */
    private final Consumer<String> logConsumer;

    /**
     * Create class instance.
     *
     * @param ignite Ignite instance.
     * @param logConsumer Logging consumer.
     */
    OptimizeMakeChangeTerminateCriteria(Ignite ignite, Consumer<String> logConsumer) {
        this.ignite = ignite;
        this.logConsumer = logConsumer;
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

        logConsumer.accept(
            "\n##########################################################################################"
                + "\n Generation: " + currGeneration
                + "\n Fittest is Chromosome Key: " + fittestChromosome
                + "\n Chromosome: " + fittestChromosome
                + "\n" + reportCoins(GAGridUtils.getGenesInOrderForChromosome(ignite, fittestChromosome))
                + "\nAvg Chromosome Fitness: " + averageFitnessScore
                + "\n##########################################################################################");

        if (!(currGeneration > 5))
            isTerminate = false;

        return isTerminate;
    }

    /**
     * Helper to print change details.
     *
     * @param genes List if Genes.
     * @return Details to print.
     */
    private String reportCoins(List<Gene> genes) {
        StringBuilder sb = new StringBuilder();

        for (Gene gene : genes) {
            sb.append("\nCoin Type: ").append(((Coin)gene.getVal()).getCoinType().toString())
                .append("\nNumber of Coins: ").append(((Coin)gene.getVal()).getNumOfCoins());
        }

        return sb.toString();
    }
}
