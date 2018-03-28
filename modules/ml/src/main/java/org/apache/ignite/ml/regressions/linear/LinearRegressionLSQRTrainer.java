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

package org.apache.ignite.ml.regressions.linear;

import java.util.Arrays;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.isolve.LinSysPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.math.isolve.lsqr.AbstractLSQR;
import org.apache.ignite.ml.math.isolve.lsqr.LSQROnHeap;
import org.apache.ignite.ml.math.isolve.lsqr.LSQRResult;

/**
 * Trainer of the linear regression model based on LSQR algorithm.
 *
 * @see AbstractLSQR
 */
public class LinearRegressionLSQRTrainer implements SingleLabelDatasetTrainer<LinearRegressionModel> {
    /** {@inheritDoc} */
    @Override public <K, V> LinearRegressionModel fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        LSQRResult res;

        try (LSQROnHeap<K, V> lsqr = new LSQROnHeap<>(
            datasetBuilder,
            new LinSysPartitionDataBuilderOnHeap<>(new FeatureExtractorWrapper<>(featureExtractor), lbExtractor)
        )) {
            res = lsqr.solve(0, 1e-12, 1e-12, 1e8, -1, false, null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        double[] x = res.getX();
        Vector weights = new DenseLocalOnHeapVector(Arrays.copyOfRange(x, 0, x.length - 1));

        return new LinearRegressionModel(weights, x[x.length - 1]);
    }

    /**
     * Feature extractor wrapper that adds additional column filled by 1.
     *
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     */
    private static class FeatureExtractorWrapper<K, V> implements IgniteBiFunction<K, V, double[]> {
        /** */
        private static final long serialVersionUID = -2686524650955735635L;

        /** Underlying feature extractor. */
        private final IgniteBiFunction<K, V, double[]> featureExtractor;

        /**
         * Constructs a new instance of feature extractor wrapper.
         *
         * @param featureExtractor Underlying feature extractor.
         */
        FeatureExtractorWrapper(IgniteBiFunction<K, V, double[]> featureExtractor) {
            this.featureExtractor = featureExtractor;
        }

        /** {@inheritDoc} */
        @Override public double[] apply(K k, V v) {
            double[] featureRow = featureExtractor.apply(k, v);
            double[] row = Arrays.copyOf(featureRow, featureRow.length + 1);

            row[featureRow.length] = 1.0;

            return row;
        }
    }
}
