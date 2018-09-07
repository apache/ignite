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

package org.apache.ignite.ml.tree.boosting;

import org.apache.ignite.ml.composition.boosting.GDBLearningStrategy;
import org.apache.ignite.ml.composition.boosting.GDBRegressionTrainer;
import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of Gradient Boosting Regression Trainer on trees.
 */
public class GDBRegressionOnTreesTrainer extends GDBRegressionTrainer {
    /** Max depth. */
    private int maxDepth;

    /** Min impurity decrease. */
    private double minImpurityDecrease;

    /** Use index structure instead of using sorting while learning. */
    private boolean usingIdx = true;

    /**
     * Constructs instance of GDBRegressionOnTreesTrainer.
     *
     * @param gradStepSize Gradient step size.
     * @param cntOfIterations Count of iterations.
     * @param maxDepth Max depth.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public GDBRegressionOnTreesTrainer(double gradStepSize, Integer cntOfIterations,
        int maxDepth, double minImpurityDecrease) {

        super(gradStepSize, cntOfIterations);
        this.maxDepth = maxDepth;
        this.minImpurityDecrease = minImpurityDecrease;
    }

    /** {@inheritDoc} */
    @NotNull @Override protected DecisionTreeRegressionTrainer buildBaseModelTrainer() {
        return new DecisionTreeRegressionTrainer(maxDepth, minImpurityDecrease).withUsingIdx(usingIdx);
    }

    /**
     * Set useIndex parameter and returns trainer instance.
     *
     * @param usingIdx Use index.
     * @return Decision tree trainer.
     */
    public GDBRegressionOnTreesTrainer withUsingIdx(boolean usingIdx) {
        this.usingIdx = usingIdx;
        return this;
    }

    /**
     * Get the max depth.
     *
     * @return The property value.
     */
    public int getMaxDepth() {
        return maxDepth;
    }

    /**
     * Set up the max depth.
     *
     * @param maxDepth The parameter value.
     * @return Decision tree trainer.
     */
    public GDBRegressionOnTreesTrainer setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return this;
    }

    /**
     * Get the min impurity decrease.
     *
     * @return The property value.
     */
    public double getMinImpurityDecrease() {
        return minImpurityDecrease;
    }

    /**
     * Set up the min impurity decrease.
     *
     * @param minImpurityDecrease The parameter value.
     * @return Decision tree trainer.
     */
    public GDBRegressionOnTreesTrainer setMinImpurityDecrease(double minImpurityDecrease) {
        this.minImpurityDecrease = minImpurityDecrease;
        return this;
    }

    /**
     * Get the using index structure property instead of using sorting during the learning process.
     *
     * @return The property value.
     */
    public boolean isUsingIdx() {
        return usingIdx;
    }

    /** {@inheritDoc} */
    @Override protected GDBLearningStrategy getLearningStrategy() {
        return new GDBOnTreesLearningStrategy(usingIdx);
    }
}
