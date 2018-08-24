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

import org.apache.ignite.ml.composition.boosting.GDBBinaryClassifierTrainer;
import org.apache.ignite.ml.composition.boosting.GDBLearningStrategy;
import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of Gradient Boosting Classifier Trainer on trees.
 */
public class GDBBinaryClassifierOnTreesTrainer extends GDBBinaryClassifierTrainer {
    /** Max depth. */
    private final int maxDepth;

    /** Min impurity decrease. */
    private final double minImpurityDecrease;

    /** Use index structure instead of using sorting while learning. */
    private boolean useIndex = true;

    /**
     * Constructs instance of GDBBinaryClassifierOnTreesTrainer.
     *
     * @param gradStepSize Gradient step size.
     * @param cntOfIterations Count of iterations.
     * @param maxDepth Max depth.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public GDBBinaryClassifierOnTreesTrainer(double gradStepSize, Integer cntOfIterations,
        int maxDepth, double minImpurityDecrease) {

        super(gradStepSize, cntOfIterations);
        this.maxDepth = maxDepth;
        this.minImpurityDecrease = minImpurityDecrease;
    }

    /** {@inheritDoc} */
    @NotNull @Override protected DecisionTreeRegressionTrainer buildBaseModelTrainer() {
        return new DecisionTreeRegressionTrainer(maxDepth, minImpurityDecrease).withUseIndex(useIndex);
    }

    /**
     * Sets useIndex parameter and returns trainer instance.
     *
     * @param useIndex Use index.
     * @return Decision tree trainer.
     */
    public GDBBinaryClassifierOnTreesTrainer withUseIndex(boolean useIndex) {
        this.useIndex = useIndex;
        return this;
    }

    /** {@inheritDoc} */
    @Override protected GDBLearningStrategy getLearningStrategy() {
        return new GDBOnTreesLearningStrategy(useIndex);
    }
}
