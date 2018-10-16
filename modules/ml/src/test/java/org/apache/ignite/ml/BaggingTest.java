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

package org.apache.ignite.ml;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.composition.BaggingModelTrainer;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.trainers.TrainerTransformers;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class BaggingTest extends TrainerTest {
//    @Test
//    public void testBaggingTrainerContextCount() {
//        Map<Integer, Double[]> cacheMock = new HashMap<>();
//
//        for (int i = 0; i < twoLinearlySeparableClasses.length; i++) {
//            double[] row = twoLinearlySeparableClasses[i];
//            Double[] convertedRow = new Double[row.length];
//            for (int j = 0; j < row.length; j++)
//                convertedRow[j] = row[j];
//            cacheMock.put(i, convertedRow);
//        }
//
//        CountTrainer countTrainer = new CountTrainer(counter);
//
//        double subsampleSize = 0.3;
//
//        new BaggingModelTrainer<Model<Vector, Vector>, >(new MeanValuePredictionsAggregator(), 10, 5, 100, 0.3) {
//
//        };
//
//        ModelsComposition model = countTrainer
//            .transform(TrainerTransformers.makeBagged(100, subsampleSize, new MeanValuePredictionsAggregator()))
//            .fit(cacheMock, parts, null, null);
//
//        Double res = model.apply(null);
//    }

    @Test
    public void testNaiveBaggingContextCount() {
        count((ctxCount, countData, integer) -> ctxCount);
    }

    @Test
    public void testNaiveBaggingDataCount() {
        count((ctxCount, countData, integer) -> countData.cnt);
    }

    protected void count(IgniteTriFunction<Long, CountData, Integer, Long> counter) {
        Map<Integer, Double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++) {
            double[] row = twoLinearlySeparableClasses[i];
            Double[] convertedRow = new Double[row.length];
            for (int j = 0; j < row.length; j++)
                convertedRow[j] = row[j];
            cacheMock.put(i, convertedRow);
        }

        CountTrainer countTrainer = new CountTrainer(counter);

        double subsampleSize = 0.3;

        ModelsComposition model = countTrainer
            .transform(TrainerTransformers.makeBagged(100, subsampleSize, new MeanValuePredictionsAggregator()))
            .fit(cacheMock, parts, null, null);

        Double res = model.apply(null);

        TestUtils.assertEquals(twoLinearlySeparableClasses.length * subsampleSize, res, twoLinearlySeparableClasses.length / 10);
    }

    protected static Long plusOfNullables(Long a, Long b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }

        return a + b;
    }

    protected static class CountTrainer extends DatasetTrainer<Model<Vector, Double>, Double> {
        private final IgniteTriFunction<Long, CountData, Integer, Long> counter;

        public CountTrainer(IgniteTriFunction<Long, CountData, Integer, Long> counter) {
            this.counter = counter;
        }

        @Override
        public <K, V> Model<Vector, Double> fit(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {
            Dataset<Long, CountData> dataset = datasetBuilder.build(
                (upstreamData, upstreamDataSize) -> upstreamDataSize,
                (upstreamData, upstreamDataSize, ctx) -> new CountData(upstreamDataSize)
            );

            Long cnt = dataset.computeWithCtx(counter, BaggingTest::plusOfNullables);

            return x -> Double.valueOf(cnt);
        }

        @Override
        protected boolean checkState(Model<Vector, Double> mdl) {
            return true;
        }

        @Override
        protected <K, V> Model<Vector, Double> updateModel(
            Model<Vector, Double> mdl,
            DatasetBuilder<K, V> datasetBuilder,
            IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {
            return fit(datasetBuilder, featureExtractor, lbExtractor);
        }
    }

    protected static class CountData implements AutoCloseable {
        private long cnt;

        public CountData(long cnt) {
            this.cnt = cnt;
        }

        @Override
        public void close() throws Exception {
            // No-op
        }
    }
}
