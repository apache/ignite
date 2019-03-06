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

package org.apache.ignite.ml.tree.boosting;

import org.apache.ignite.ml.composition.boosting.GDBBinaryClassifierTrainer;
import org.apache.ignite.ml.composition.boosting.GDBLearningStrategy;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of Gradient Boosting Classifier Trainer on trees.
 */
public class GDBBinaryClassifierOnTreesTrainer extends GDBBinaryClassifierTrainer {
    /** Max depth. */
    private int maxDepth;

    /** Min impurity decrease. */
    private double minImpurityDecrease;

    /** Use index structure instead of using sorting during the learning process. */
    private boolean usingIdx = true;

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
        return new DecisionTreeRegressionTrainer(maxDepth, minImpurityDecrease).withUsingIdx(usingIdx);
    }

    /** {@inheritDoc} */
    @Override protected GDBLearningStrategy getLearningStrategy() {
        return new GDBOnTreesLearningStrategy(usingIdx);
    }

    /** {@inheritDoc} */
    @Override public GDBBinaryClassifierOnTreesTrainer withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (GDBBinaryClassifierOnTreesTrainer)super.withEnvironmentBuilder(envBuilder);
    }

    /**
     * Set useIndex parameter and returns trainer instance.
     *
     * @param usingIdx Use index.
     * @return Decision tree trainer.
     */
    public GDBBinaryClassifierOnTreesTrainer withUsingIdx(boolean usingIdx) {
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
    public GDBBinaryClassifierOnTreesTrainer setMaxDepth(int maxDepth) {
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
    public GDBBinaryClassifierOnTreesTrainer setMinImpurityDecrease(double minImpurityDecrease) {
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
}
