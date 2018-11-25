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

package org.apache.ignite.ml.preprocessing.standardscaling;

import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;

/**
 * Trainer of the standard scaler preprocessor.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class StandardScalerTrainer<K, V> implements PreprocessingTrainer<K, V, Vector, Vector> {
    /** {@inheritDoc} */
    @Override public StandardScalerPreprocessor<K, V> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> basePreprocessor) {

        StandardScalerData standardScalerData = computeSum(datasetBuilder, basePreprocessor);

        int n = standardScalerData.sum.length;
        long count = standardScalerData.count;
        double[] mean = new double[n];
        double[] sigma = new double[n];

        for (int i = 0; i < n; i++) {
            mean[i] = standardScalerData.sum[i] / count;
            double variace = (standardScalerData.squaredSum[i] - Math.pow(standardScalerData.sum[i], 2) / count) / count;
            sigma[i] = Math.sqrt(variace);
        }
        return new StandardScalerPreprocessor<>(mean, sigma, basePreprocessor);
    }

    /** Computes sum, squared sum and row count. */
    private StandardScalerData computeSum(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> basePreprocessor) {
        try (Dataset<EmptyContext, StandardScalerData> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            (upstream, upstreamSize, ctx) -> {
                double[] sum = null;
                double[] squaredSum = null;
                long count = 0;

                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();
                    Vector row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                    if (sum == null) {
                        sum = new double[row.size()];
                        squaredSum = new double[row.size()];
                    }
                    else {
                        assert sum.length == row.size() : "Base preprocessor must return exactly " + sum.length
                            + " features";
                    }

                    ++count;
                    for (int i = 0; i < row.size(); i++) {
                        double x = row.get(i);
                        sum[i] += x;
                        squaredSum[i] += x * x;
                    }
                }
                return new StandardScalerData(sum, squaredSum, count);
            }
        )) {

            return dataset.compute(data -> data,
                (a, b) -> {
                    if (a == null)
                        return b;
                    if (b == null)
                        return a;

                    return a.merge(b);
                });
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
