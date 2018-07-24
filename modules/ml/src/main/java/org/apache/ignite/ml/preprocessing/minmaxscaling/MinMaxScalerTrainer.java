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

package org.apache.ignite.ml.preprocessing.minmaxscaling;

import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;

/**
 * Trainer of the minmaxscaling preprocessor.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class MinMaxScalerTrainer<K, V> implements PreprocessingTrainer<K, V, Vector, Vector> {
    /** {@inheritDoc} */
    @Override public MinMaxScalerPreprocessor<K, V> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> basePreprocessor) {
        try (Dataset<EmptyContext, MinMaxScalerPartitionData> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            (upstream, upstreamSize, ctx) -> {
                double[] min = null;
                double[] max = null;

                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();
                    Vector row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                    if (min == null) {
                        min = new double[row.size()];
                        for (int i = 0; i < min.length; i++)
                            min[i] = Double.MAX_VALUE;
                    }
                    else
                        assert min.length == row.size() : "Base preprocessor must return exactly " + min.length
                            + " features";

                    if (max == null) {
                        max = new double[row.size()];
                        for (int i = 0; i < max.length; i++)
                            max[i] = -Double.MAX_VALUE;
                    }
                    else
                        assert max.length == row.size() : "Base preprocessor must return exactly " + min.length
                            + " features";

                    for (int i = 0; i < row.size(); i++) {
                        if (row.get(i) < min[i])
                            min[i] = row.get(i);
                        if (row.get(i) > max[i])
                            max[i] = row.get(i);
                    }
                }

                return new MinMaxScalerPartitionData(min, max);
            }
        )) {
            double[][] minMax = dataset.compute(
                data -> data.getMin() != null ? new double[][]{ data.getMin(), data.getMax() } : null,
                (a, b) -> {
                    if (a == null)
                        return b;

                    if (b == null)
                        return a;

                    double[][] res = new double[2][];

                    res[0] = new double[a[0].length];
                    for (int i = 0; i < res[0].length; i++)
                        res[0][i] = Math.min(a[0][i], b[0][i]);

                    res[1] = new double[a[1].length];
                    for (int i = 0; i < res[1].length; i++)
                        res[1][i] = Math.max(a[1][i], b[1][i]);

                    return res;
                }
            );

            return new MinMaxScalerPreprocessor<>(minMax[0], minMax[1], basePreprocessor);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
