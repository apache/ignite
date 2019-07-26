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

package org.apache.ignite.ml.selection.paramgrid;

import org.apache.ignite.ml.util.genetic.CrossoverStrategy;
import org.apache.ignite.ml.util.genetic.SelectionStrategy;

/** Represents the Genetic algorithms usage for finding the best set of hyper-parameters. */
public class EvolutionOptimizationStrategy extends HyperParameterTuningStrategy {
    /** Seed. */
    private long seed = 1234L;

    /** Amount of elite chromosomes. */
    private int amountOfEliteChromosomes = 2;

    /** Amount of generations. */
    private int amountOfGenerations = 10;

    /** Crossingover probability. */
    private double crossingoverProbability = 0.9;

    /** Mutation probability. */
    private double mutationProbability = 0.1;

    /** Crossover strategy. */
    private CrossoverStrategy crossoverStgy = CrossoverStrategy.UNIFORM;

    /** Selection strategy. */
    private SelectionStrategy selectionStgy = SelectionStrategy.ROULETTE_WHEEL;

    /**
     *
     */
    public long getSeed() {
        return seed;
    }

    /**
     *
     */
    public int getAmountOfGenerations() {
        return amountOfGenerations;
    }

    /**
     *
     */
    public int getAmountOfEliteChromosomes() {
        return amountOfEliteChromosomes;
    }

    /**
     *
     */
    public double getCrossingoverProbability() {
        return crossingoverProbability;
    }

    /**
     *
     */
    public double getMutationProbability() {
        return mutationProbability;
    }

    /**
     *
     */
    public CrossoverStrategy getCrossoverStgy() {
        return crossoverStgy;
    }

    /**
     *
     */
    public SelectionStrategy getSelectionStgy() {
        return selectionStgy;
    }

    /**
     * Set up the seed number.
     *
     * @param seed Seed.
     */
    public EvolutionOptimizationStrategy withSeed(long seed) {
        this.seed = seed;
        return this;
    }

    /**
     * @param crossingoverProbability Crossingover probability.
     */
    public EvolutionOptimizationStrategy withCrossingoverProbability(double crossingoverProbability) {
        this.crossingoverProbability = crossingoverProbability;
        return this;
    }

    /**
     * @param mutationProbability Mutation probability.
     */
    public EvolutionOptimizationStrategy withMutationProbability(double mutationProbability) {
        this.mutationProbability = mutationProbability;
        return this;
    }

    /**
     * @param crossoverStgy Crossover strategy.
     */
    public EvolutionOptimizationStrategy withCrossoverStgy(CrossoverStrategy crossoverStgy) {
        this.crossoverStgy = crossoverStgy;
        return this;
    }

    /**
     * @param selectionStgy Selection strategy.
     */
    public EvolutionOptimizationStrategy withSelectionStgy(SelectionStrategy selectionStgy) {
        this.selectionStgy = selectionStgy;
        return this;
    }


    /** {@inheritDoc} */
    @Override public String getName() {
        return "Evolution Optimization";
    }
}
