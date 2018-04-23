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
 * This example demonstrates how to create a IFitnessFunction <br/>
 *
 * Your IFitness function will vary depending on your particular use case. <br/>
 *
 * For this fitness function, we simply want to calculate the value of  <br/>
 *
 * an individual solution relative to other solutions. <br/>
 */
public class OptimizeMakeChangeFitnessFunction implements IFitnessFunction {
    /** target amount */
    int targetAmount = 0;

    /**
     * @param targetAmount Amount of change
     */
    public OptimizeMakeChangeFitnessFunction(int targetAmount) {
        this.targetAmount = targetAmount;
    }

    /**
     * Calculate fitness
     *
     * @param genes Genes
     * @return Fitness value
     */
    public double evaluate(List<Gene> genes) {

        int changeAmount = getAmountOfChange(genes);
        int totalCoins = getTotalNumberOfCoins(genes);
        int changeDifference = Math.abs(targetAmount - changeAmount);

        double fitness = (99 - changeDifference);

        if (changeAmount == targetAmount) {
            fitness += 100 - (10 * totalCoins);
        }

        return fitness;

    }

    /**
     * Calculate amount of change
     *
     * @param genes Genes
     * @return Amount of change
     */
    private int getAmountOfChange(List<Gene> genes) {
        Gene quarterGene = (Gene)genes.get(0);
        Gene dimeGene = (Gene)genes.get(1);
        Gene nickelGene = (Gene)genes.get(2);
        Gene pennyGene = (Gene)genes.get(3);

        int numQuarters = ((Coin)quarterGene.getValue()).getNumberOfCoins();
        int numDimes = ((Coin)dimeGene.getValue()).getNumberOfCoins();
        int numNickels = ((Coin)nickelGene.getValue()).getNumberOfCoins();
        int numPennies = ((Coin)pennyGene.getValue()).getNumberOfCoins();

        return (numQuarters * 25) + (numDimes * 10) + (numNickels * 5) + numPennies;
    }

    /**
     * Return the total number of coins
     *
     * @param genes Genes
     * @return Number of coins
     */
    private int getTotalNumberOfCoins(List<Gene> genes) {

        int totalNumberOfCoins = 0;

        for (Gene gene : genes) {
            int numberOfCoins = ((Coin)gene.getValue()).getNumberOfCoins();
            totalNumberOfCoins = totalNumberOfCoins + numberOfCoins;

        }
        return totalNumberOfCoins;

    }
}
