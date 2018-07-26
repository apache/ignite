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
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.data.SimpleLabeledDatasetDataBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.isolve.lsqr.AbstractLSQR;
import org.apache.ignite.ml.math.isolve.lsqr.LSQROnHeap;
import org.apache.ignite.ml.math.isolve.lsqr.LSQRResult;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * Trainer of the linear regression model based on LSQR algorithm.
 *
 * @see AbstractLSQR
 */
public class LinearRegressionLSQRTrainer extends SingleLabelDatasetTrainer<LinearRegressionModel> {
    /** {@inheritDoc} */
    @Override public <K, V> LinearRegressionModel fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        LSQRResult res;

        try (LSQROnHeap<K, V> lsqr = new LSQROnHeap<>(
            datasetBuilder,
            new SimpleLabeledDatasetDataBuilder<>(
                new FeatureExtractorWrapper<>(featureExtractor),
                lbExtractor.andThen(e -> new double[]{e})
            )
        )) {
            res = lsqr.solve(0, 1e-12, 1e-12, 1e8, -1, false, null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        double[] x = res.getX();
        Vector weights = new DenseVector(Arrays.copyOfRange(x, 0, x.length - 1));

        return new LinearRegressionModel(weights, x[x.length - 1]);
    }
}
