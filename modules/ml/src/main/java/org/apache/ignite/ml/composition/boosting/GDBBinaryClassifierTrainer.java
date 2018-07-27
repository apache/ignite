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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;

/**
 * Trainer for binary classifier using Gradient Boosting.
 * As preparing stage this algorithm learn labels in dataset and create mapping dataset labels to 0 and 1.
 * This algorithm uses gradient of Logarithmic Loss metric [LogLoss] by default in each step of learning.
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
        super(gradStepSize,
            cntOfIterations,
            LossGradientPerPredictionFunctions.LOG_LOSS);
    }

    /**
     * Constructs instance of GDBBinaryClassifierTrainer.
     *
     * @param gradStepSize Grad step size.
     * @param cntOfIterations Count of learning iterations.
     * @param lossGradient Gradient of loss function. First argument is sample size, second argument is valid answer, third argument is current model prediction.
     */
    public GDBBinaryClassifierTrainer(double gradStepSize,
        Integer cntOfIterations,
        IgniteTriFunction<Long, Double, Double, Double> lossGradient) {

        super(gradStepSize, cntOfIterations, lossGradient);
    }

    /** {@inheritDoc} */
    @Override protected <V, K> void learnLabels(DatasetBuilder<K, V> builder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lExtractor) {

        List<Double> uniqLabels = new ArrayList<Double>(
            builder.build(new EmptyContextBuilder<>(), new LabeledDatasetPartitionDataBuilderOnHeap<>(featureExtractor, lExtractor))
                .compute((IgniteFunction<LabeledDataset<Double,LabeledVector>, Set<Double>>) x ->
                    Arrays.stream(x.labels()).boxed().collect(Collectors.toSet()), (a, b) -> {
                        if (a == null)
                            return b;
                        if (b == null)
                            return a;
                        a.addAll(b);
                        return a;
                    }
                ));

        A.ensure(uniqLabels.size() == 2, "Binary classifier expects two types of labels in learning dataset");
        externalFirstCls = uniqLabels.get(0);
        externalSecondCls = uniqLabels.get(1);
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
}
