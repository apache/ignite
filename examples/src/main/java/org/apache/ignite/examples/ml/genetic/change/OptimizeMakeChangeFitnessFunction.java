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

package org.apache.ignite.examples.ml.genetic.change;

import java.util.List;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.IFitnessFunction;

/**
 * This example demonstrates how to create a {@link IFitnessFunction}.
 * <p>
 * Your fitness function will vary depending on your particular use case. For this fitness function, we simply want
 * to calculate the value of an individual solution relative to other solutions.</p>
 */
public class OptimizeMakeChangeFitnessFunction implements IFitnessFunction {
    /** Target amount. */
    private int targetAmount;

    /**
     * @param targetAmount Amount of change.
     */
    public OptimizeMakeChangeFitnessFunction(int targetAmount) {
        this.targetAmount = targetAmount;
    }

    /**
     * Calculate fitness.
     *
     * @param genes List of genes.
     * @return Fitness value.
     */
    public double evaluate(List<Gene> genes) {
        int changeAmount = getAmountOfChange(genes);
        int totalCoins = getTotalNumberOfCoins(genes);
        int changeDifference = Math.abs(targetAmount - changeAmount);

        double fitness = (99 - changeDifference);

        if (changeAmount == targetAmount)
            fitness += 100 - (10 * totalCoins);

        return fitness;
    }

    /**
     * Calculate amount of change.
     *
     * @param genes List of genes.
     * @return Amount of change.
     */
    private int getAmountOfChange(List<Gene> genes) {
        Gene quarterGene = genes.get(0);
        Gene dimeGene = genes.get(1);
        Gene nickelGene = genes.get(2);
        Gene pennyGene = genes.get(3);

        int numQuarters = ((Coin)quarterGene.getVal()).getNumOfCoins();
        int numDimes = ((Coin)dimeGene.getVal()).getNumOfCoins();
        int numNickels = ((Coin)nickelGene.getVal()).getNumOfCoins();
        int numPennies = ((Coin)pennyGene.getVal()).getNumOfCoins();

        return (numQuarters * 25) + (numDimes * 10) + (numNickels * 5) + numPennies;
    }

    /**
     * Return the total number of coins.
     *
     * @param genes List of genes.
     * @return Number of coins.
     */
    private int getTotalNumberOfCoins(List<Gene> genes) {
        int totalNumOfCoins = 0;

        for (Gene gene : genes) {
            int numOfCoins = ((Coin)gene.getVal()).getNumOfCoins();
            totalNumOfCoins = totalNumOfCoins + numOfCoins;
        }

        return totalNumOfCoins;
    }
}
