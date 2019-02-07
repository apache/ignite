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
