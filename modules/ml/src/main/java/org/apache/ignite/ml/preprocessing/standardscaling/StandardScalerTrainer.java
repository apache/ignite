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

import java.util.Arrays;
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

        MeanHelper meanHelper = calculateMeans(datasetBuilder, basePreprocessor);
        VarianceHelper varianceHelper = calculateVariance(datasetBuilder, basePreprocessor, meanHelper);
        double[] sigma = new double[meanHelper.means.length];
        for (int i = 0; i < varianceHelper.vars.length; i++) {
            sigma[i] = Math.sqrt(varianceHelper.vars[i]);
        }
        return new StandardScalerPreprocessor<>(meanHelper.means, sigma, basePreprocessor);
    }

    private MeanHelper calculateMeans(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> basePreprocessor) {
        try (Dataset<EmptyContext, MeanHelper> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            (upstream, upstreamSize, ctx) -> {
                double[] sum = null;
                long count = 0;
                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();
                    Vector row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                    if (sum == null) {
                        sum = new double[row.size()];
                        Arrays.fill(sum, .0);
                    }
                    else
                        assert sum.length == row.size() : "Base preprocessor must return exactly " + sum.length
                            + " features";

                    ++count;
                    for (int i = 0; i < row.size(); i++) {
                        sum[i] += row.get(i);
                    }
                }
                return new MeanHelper(sum, count);
            }
        )) {
            MeanHelper helper = dataset.compute(data -> data,
                (a, b) -> {
                    if (a == null)
                        return b;
                    if (b == null)
                        return a;

                    return a.merge(b);
                });

            for (int i = 0; i < helper.means.length; i++) {
                helper.means[i] /= helper.count;
            }

            return helper;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    VarianceHelper calculateVariance(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> basePreprocessor, MeanHelper meansHelper) {
        double[] means = meansHelper.means;
        try (Dataset<EmptyContext, VarianceHelper> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            (upstream, upstreamSize, ctx) -> {
                double[] squaredDiff = null;
                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();
                    Vector row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                    if (squaredDiff == null) {
                        squaredDiff = new double[row.size()];
                        Arrays.fill(squaredDiff, .0);
                    }
                    else
                        assert squaredDiff.length == row.size() : "Base preprocessor must return exactly " + squaredDiff.length
                            + " features";

                    for (int i = 0; i < row.size(); i++) {
                        squaredDiff[i] += (means[i] - row.get(i)) * (means[i] - row.get(i));
                    }
                }
                return new VarianceHelper(squaredDiff);
            }
        )) {
            VarianceHelper helper = dataset.compute(data -> data,
                (a, b) -> {
                    if (a == null)
                        return b;
                    if (b == null)
                        return a;

                    return a.merge(b);
                });

            for (int i = 0; i < helper.vars.length; i++) {
                helper.vars[i] /= meansHelper.count;
            }

            return helper;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class MeanHelper implements AutoCloseable {
        double[] means;
        long count;

        public MeanHelper(double[] means, long count) {
            this.means = means;
            this.count = count;
        }

        public double[] getMeans() {
            return means;
        }

        MeanHelper merge(MeanHelper that) {
            for (int i = 0; i < means.length; i++) {
                means[i] += that.means[i];
            }
            count += that.count;
            return this;
        }

        /** */
        @Override public void close() {
            // Do nothing, GC will clean up.
        }
    }

    private static class VarianceHelper implements AutoCloseable {
        double[] vars;

        public VarianceHelper(double[] vars) {
            this.vars = vars;
        }

        public double[] getMeans() {
            return vars;
        }

        VarianceHelper merge(VarianceHelper that) {
            for (int i = 0; i < vars.length; i++) {
                vars[i] += that.vars[i];
            }
            return this;
        }

        /** */
        @Override public void close() {
            // Do nothing, GC will clean up.
        }
    }
}
