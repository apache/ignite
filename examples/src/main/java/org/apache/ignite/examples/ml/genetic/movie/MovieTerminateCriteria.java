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

package org.apache.ignite.examples.ml.genetic.movie;

import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.genetic.Chromosome;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.parameter.ITerminateCriteria;
import org.apache.ignite.ml.genetic.utils.GAGridUtils;

/**
 * Represents the terminate condition for {@link MovieGAExample}.
 * <p>
 * Class terminates Genetic algorithm when fitness score is more than 32.</p>
 */
public class MovieTerminateCriteria implements ITerminateCriteria {
    /** Ignite instance. */
    private final Ignite ignite;

    /** */
    private final Consumer<String> logConsumer;

    /**
     * Create class instance.
     *
     * @param ignite Ignite instance.
     * @param logConsumer Logging consumer.
     */
    MovieTerminateCriteria(Ignite ignite, Consumer<String> logConsumer) {
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
                + "\nChromosome: " + fittestChromosome
                + "\n" + reportMovies(GAGridUtils.getGenesInOrderForChromosome(ignite, fittestChromosome))
                + "\n##########################################################################################");

        if (!(fittestChromosome.getFitnessScore() > 32))
            isTerminate = false;

        return isTerminate;
    }

    /**
     * Helper to print movies details.
     *
     * @param genes List of Genes.
     * @return Movies details.
     */
    private String reportMovies(List<Gene> genes) {
        StringBuilder sb = new StringBuilder();

        for (Gene gene : genes) {
            sb.append("\nName: ").append(((Movie)gene.getVal()).getName())
                .append("\nGenres: ").append(((Movie)gene.getVal()).getGenre().toString())
                .append("\nIMDB Rating: ").append(((Movie)gene.getVal()).getImdbRating());
        }

        return sb.toString();
    }
}
