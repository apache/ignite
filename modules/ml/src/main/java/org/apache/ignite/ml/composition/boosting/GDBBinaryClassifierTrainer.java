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

package org.apache.ignite.ml.composition.boosting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.ml.composition.boosting.loss.LogLoss;
import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;

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
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lExtractor) {

        Set<Double> uniqLabels = builder.build(new EmptyContextBuilder<>(), new LabeledDatasetPartitionDataBuilderOnHeap<>(featureExtractor, lExtractor))
            .compute((IgniteFunction<LabeledVectorSet<Double, LabeledVector>, Set<Double>>)x ->
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
}
