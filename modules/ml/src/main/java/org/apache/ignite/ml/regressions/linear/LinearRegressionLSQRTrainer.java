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
import org.apache.ignite.ml.DatasetTrainer;
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
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 *
 * @see AbstractLSQR
 */
public class LinearRegressionLSQRTrainer<K, V> implements DatasetTrainer<K, V, LinearRegressionModel> {
    /** {@inheritDoc} */
    @Override public LinearRegressionModel fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor, int cols) {

        LSQRResult res;

        try (LSQROnHeap<K, V> lsqr = new LSQROnHeap<>(
            datasetBuilder,
            new LinSysPartitionDataBuilderOnHeap<>(
                (k, v) -> {
                    double[] row = Arrays.copyOf(featureExtractor.apply(k, v), cols + 1);

                    row[cols] = 1.0;

                    return row;
                },
                lbExtractor,
                cols + 1
            )
        )) {
            res = lsqr.solve(0, 1e-12, 1e-12, 1e8, -1, false, null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        Vector weights = new DenseLocalOnHeapVector(Arrays.copyOfRange(res.getX(), 0, cols));

        return new LinearRegressionModel(weights, res.getX()[cols]);
    }
}
