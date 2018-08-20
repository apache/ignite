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

package org.apache.ignite.examples.ml.genetic.helloworld;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.ml.genetic.Chromosome;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.parameter.ITerminateCriteria;
import org.apache.ignite.ml.genetic.utils.GAGridUtils;

/**
 * Represents the terminate condition for {@link HelloWorldGAExample}.
 * <p>
 * Class terminates Genetic algorithm when fitness score is more than 10.</p>
 */
public class HelloWorldTerminateCriteria implements ITerminateCriteria {
    /** Ignite logger. */
    private IgniteLogger igniteLog;
    /** Ignite instance. */
    private Ignite ignite;

    /**
     * Create class instance.
     *
     * @param ignite Ignite instance.
     */
    public HelloWorldTerminateCriteria(Ignite ignite) {
        this.ignite = ignite;
        this.igniteLog = ignite.log();
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
        igniteLog.info("Chromosome: " + fittestChromosome);
        printPhrase(GAGridUtils.getGenesInOrderForChromosome(ignite, fittestChromosome));
        igniteLog.info("Avg Chromosome Fitness: " + averageFitnessScore);
        igniteLog.info("##########################################################################################");

        if (!(fittestChromosome.getFitnessScore() > 10))
            isTerminate = false;

        return isTerminate;
    }

    /**
     * Helper to print phrase.
     *
     * @param genes List of Genes.
     */
    private void printPhrase(List<Gene> genes) {
        StringBuilder sbPhrase = new StringBuilder();

        for (Gene gene : genes)
            sbPhrase.append(((Character)gene.getVal()).toString());

        igniteLog.info(sbPhrase.toString());
    }
}
