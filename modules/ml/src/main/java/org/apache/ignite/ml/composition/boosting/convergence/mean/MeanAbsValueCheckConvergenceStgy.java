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

package org.apache.ignite.ml.composition.boosting.convergence.mean;

import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckStrategy;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.data.DecisionTreeData;

/**
 * Use mean value of errors for estimating error on dataset.
 *
 * @param <K> Type of a key in upstream data.
 * @param <V> Type of a value in upstream data.
 */
public class MeanAbsValueCheckConvergenceStgy<K,V> extends ConvergenceCheckStrategy<K,V> {
    /** Serial version uid. */
    private static final long serialVersionUID = 8534776439755210864L;

    /**
     * Creates an intance of MeanAbsValueCheckConvergenceStgy.
     *
     * @param sampleSize Sample size.
     * @param externalLbToInternalMapping External label to internal mapping.
     * @param lossGradient Loss gradient.
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     */
    public MeanAbsValueCheckConvergenceStgy(long sampleSize,
        IgniteFunction<Double, Double> externalLbToInternalMapping,
        IgniteTriFunction<Long, Double, Double, Double> lossGradient,
        DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor,
        double precision) {

        super(sampleSize, externalLbToInternalMapping, lossGradient, datasetBuilder, featureExtractor, lbExtractor, precision);
    }

    /** {@inheritDoc} */
    @Override public Double computeMeanErrorOnDataset(Dataset<EmptyContext, ? extends DecisionTreeData> dataset,
        ModelsComposition mdl) {

        IgniteBiTuple<Double, Long> sumAndCnt = dataset.compute(partition -> {
            Double sum = 0.0;

            for(int i = 0; i < partition.getFeatures().length; i++) {
                double error = computeError(VectorUtils.of(partition.getFeatures()[i]), partition.getLabels()[i], mdl);
                sum += Math.abs(error);
            }

            return new IgniteBiTuple<>(sum, (long) partition.getLabels().length);
        }, (left, right) -> {
            if (left == null) {
                if (right != null)
                    return right;
                else
                    return new IgniteBiTuple<>(0.0, 0L);
            }

            if (right == null)
                return left;

            return new IgniteBiTuple<>(
                left.getKey() + right.getKey(),
                right.getValue() + left.getValue()
            );
        });

        if(sumAndCnt == null || sumAndCnt.getValue() == 0)
            return Double.NaN;
        return sumAndCnt.getKey() / sumAndCnt.getValue();
    }
}
