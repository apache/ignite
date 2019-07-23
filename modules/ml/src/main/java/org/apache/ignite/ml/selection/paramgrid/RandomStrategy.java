/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.ignite.ml.selection.paramgrid;

/**
 * This strategy enables the random search in hyper-parameter space.
 */
public class RandomStrategy extends HyperParameterTuningStrategy {
    /** Satisfactory fitness to stop the hyperparameter search. */
    private double satisfactoryFitness = 0.5;

    /** Max tries to stop the hyperparameter search. */
    private int maxTries = 100;

    /** Seed. */
    private long seed = 1234L;

    /** Returns the seed. */
    public long getSeed() {
        return seed;
    }

    /**
     * Set up the seed number.
     *
     * @param seed Seed.
     */
    public RandomStrategy withSeed(long seed) {
        this.seed = seed;
        return this;
    }

    /**
     *
     */
    public double getSatisfactoryFitness() {
        return satisfactoryFitness;
    }

    /**
     * Set up the satisfactory fitness to stop the hyperparameter search.
     *
     * @param fitness Fitness.
     */
    public RandomStrategy withSatisfactoryFitness(double fitness) {
        satisfactoryFitness = fitness;
        return this;
    }

    /** Returns the max number of tries to stop the hyperparameter search. */
    public int getMaxTries() {
        return maxTries;
    }

    /**
     * Set up the max number of tries to stop the hyperparameter search.
     *
     * @param maxTries Max tries.
     */
    public RandomStrategy withMaxTries(int maxTries) {
        this.maxTries = maxTries;
        return this;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return "Random Search";
    }
}
