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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;

import org.apache.ignite.ml.genetic.Chromosome;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.parameter.ITerminateCriteria;
import org.apache.ignite.ml.genetic.utils.GAGridUtils;

/**
 * Terminate Condition implementation for OptimizeMakeChangeGATest <br/>
 */
public class OptimizeMakeChangeTerminateCriteria implements ITerminateCriteria {
    /** Ignite logger */
    private IgniteLogger igniteLogger = null;
    /** Ignite instance */
    private Ignite ignite = null;

    /**
     * @param ignite Ignite
     */
    public OptimizeMakeChangeTerminateCriteria(Ignite ignite) {
        this.ignite = ignite;
        this.igniteLogger = ignite.log();
    }

    /**
     * @param fittestChromosome Most fit chromosome at for the nth generation
     * @param averageFitnessScore Average fitness score as of the nth generation
     * @param currentGeneration Current generation
     * @return Boolean value
     */
    public boolean isTerminationConditionMet(Chromosome fittestChromosome, double averageFitnessScore,
        int currentGeneration) {
        boolean isTerminate = true;

        igniteLogger.info("##########################################################################################");
        igniteLogger.info("Generation: " + currentGeneration);
        igniteLogger.info("Fittest is Chromosome Key: " + fittestChromosome);
        igniteLogger.info("Chromsome: " + fittestChromosome);
        printCoins(GAGridUtils.getGenesInOrderForChromosome(ignite, fittestChromosome));
        igniteLogger.info("Avg Chromsome Fitness: " + averageFitnessScore);
        igniteLogger.info("##########################################################################################");

        if (!(currentGeneration > 5)) {
            isTerminate = false;
        }

        return isTerminate;
    }

    /**
     * Helper to print change detail
     *
     * @param genes List if Genes
     */
    private void printCoins(List<Gene> genes) {
        for (Gene gene : genes) {
            igniteLogger.info("Coin Type: " + ((Coin)gene.getValue()).getCoinType().toString());
            igniteLogger.info("Number of Coins: " + ((Coin)gene.getValue()).getNumberOfCoins());
        }

    }
}
