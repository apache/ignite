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
