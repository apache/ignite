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

package org.apache.ignite.ml.tree;

import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.mse.MSEImpurityMeasure;
import org.apache.ignite.ml.tree.impurity.mse.MSEImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.util.StepFunctionCompressor;
import org.apache.ignite.ml.tree.leaf.MeanDecisionTreeLeafBuilder;

/**
 * Decision tree regressor based on distributed decision tree trainer that allows to fit trees using row-partitioned
 * dataset.
 */
public class DecisionTreeRegressionTrainer extends DecisionTree<MSEImpurityMeasure> {
    /**
     * Constructs a new decision tree regressor with default impurity function compressor.
     *
     * @param maxDeep Max tree deep.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public DecisionTreeRegressionTrainer(int maxDeep, double minImpurityDecrease) {
        this(maxDeep, minImpurityDecrease, null);
    }

    /**
     * Constructs a new decision tree regressor.
     *
     * @param maxDeep Max tree deep.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public DecisionTreeRegressionTrainer(int maxDeep, double minImpurityDecrease,
        StepFunctionCompressor<MSEImpurityMeasure> compressor) {
        super(maxDeep, minImpurityDecrease, compressor, new MeanDecisionTreeLeafBuilder());
    }

    /**
     * Sets usingIdx parameter and returns trainer instance.
     *
     * @param usingIdx Use index.
     * @return Decision tree trainer.
     */
    public DecisionTreeRegressionTrainer withUsingIdx(boolean usingIdx) {
        this.usingIdx = usingIdx;
        return this;
    }

    /** {@inheritDoc} */
    @Override protected ImpurityMeasureCalculator<MSEImpurityMeasure> getImpurityMeasureCalculator(
        Dataset<EmptyContext, DecisionTreeData> dataset) {

        return new MSEImpurityMeasureCalculator(usingIdx);
    }

    /** {@inheritDoc} */
    @Override public DecisionTreeRegressionTrainer withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (DecisionTreeRegressionTrainer)super.withEnvironmentBuilder(envBuilder);
    }
}
