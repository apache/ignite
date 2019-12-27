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

package org.apache.ignite.ml.composition.boosting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.ml.composition.boosting.loss.LogLoss;
import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.tree.boosting.GDBBinaryClassifierOnTreesTrainer;

/**
 * Trainer for binary classifier using Gradient Boosting. As preparing stage this algorithm learn labels in dataset and
 * create mapping dataset labels to 0 and 1. This algorithm uses gradient of Logarithmic Loss metric [LogLoss] by
 * default in each step of learning.
 */
public abstract class GDBBinaryClassifierTrainer extends GDBTrainer {
    /** External representation of first class. */
    private double externalFirstCls; //internal 0.0

    /** External representation of second class. */
    private double externalSecondCls; //internal 1.0

    /**
     * Constructs instance of GDBBinaryClassifierTrainer.
     *
     * @param gradStepSize Grad step size.
     * @param cntOfIterations Count of learning iterations.
     */
    public GDBBinaryClassifierTrainer(double gradStepSize, Integer cntOfIterations) {
        super(gradStepSize, cntOfIterations, new LogLoss());
    }

    /**
     * Constructs instance of GDBBinaryClassifierTrainer.
     *
     * @param gradStepSize Grad step size.
     * @param cntOfIterations Count of learning iterations.
     * @param loss Loss function.
     */
    public GDBBinaryClassifierTrainer(double gradStepSize, Integer cntOfIterations, Loss loss) {
        super(gradStepSize, cntOfIterations, loss);
    }

    /** {@inheritDoc} */
    @Override protected <V, K> boolean learnLabels(DatasetBuilder<K, V> builder,
        Preprocessor<K, V> preprocessor) {
        learningEnvironment().initDeployingContext(preprocessor);

        Set<Double> uniqLabels = builder.build(
            envBuilder,
            new EmptyContextBuilder<>(),
            new LabeledDatasetPartitionDataBuilderOnHeap<>(preprocessor),
            learningEnvironment()
        ).compute((IgniteFunction<LabeledVectorSet<LabeledVector>, Set<Double>>)x ->
                        Arrays.stream(x.labels()).boxed().collect(Collectors.toSet()), (a, b) -> {
                    if (a == null)
                        return b;
                    if (b == null)
                        return a;
                    a.addAll(b);
                    return a;
                }
        );

        if (uniqLabels != null && uniqLabels.size() == 2) {
            ArrayList<Double> lblsArr = new ArrayList<>(uniqLabels);
            externalFirstCls = lblsArr.get(0);
            externalSecondCls = lblsArr.get(1);
            return true;
        }
        else
            return false;
    }

    /** {@inheritDoc} */
    @Override protected double externalLabelToInternal(double x) {
        return x == externalFirstCls ? 0.0 : 1.0;
    }

    /** {@inheritDoc} */
    @Override protected double internalLabelToExternal(double indent) {
        double sigma = 1.0 / (1.0 + Math.exp(-indent));
        double internalCls = sigma < 0.5 ? 0.0 : 1.0;
        return internalCls == 0.0 ? externalFirstCls : externalSecondCls;
    }

    /** {@inheritDoc} */
    @Override public GDBBinaryClassifierOnTreesTrainer withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (GDBBinaryClassifierOnTreesTrainer)super.withEnvironmentBuilder(envBuilder);
    }
}
