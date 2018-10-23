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

import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.composition.BaggingModelTrainer;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.trainers.TrainerTransformers;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BaggingTest extends TrainerTest {
    @Test
    public void testBaggingTrainerContextCount() {
        Map<Integer, Double[]> cacheMock = getCacheMock();

        double subsampleSize = 0.3;

        BaggingModelTrainer<Model<Vector, Double>, Integer, Void> trainer =
            new BaggingModelTrainer<Model<Vector, Double>, Integer, Void>(
                new MeanValuePredictionsAggregator(),
                10,
                5,
                100,
                0.3) {
            @Override
            protected boolean checkState(ModelsComposition mdl) {
                return true;
            }

            @Override
            protected Model<Vector, Double> init() {
                return new ConstModel(0);
            }

            @Override
            protected Integer trainingIteration(
                int partitionIdx,
                BootstrappedDatasetPartition part,
                int modelIdx,
                Set<Integer> subspace,
                Void meta) {
                // TODO: parametrize with function here.
                int res = 0;
                for (int i = 0; i < part.getRowsCount(); i++) {
                    res += part.getRow(i).counters()[modelIdx];
                }
                return res;
            }

            @Override
            protected Integer reduceTrainingResults(Integer res1, Integer res2) {
                return res1 + res2;
            }

            @Override
            protected Integer identity() {
                return 0;
            }

            @Override
            protected Model<Vector, Double> applyTrainingResultsToModel(Model<Vector, Double> model, Integer res) {
                return new ConstModel(res);
            }

            @Override
            protected Void getMeta(List<Model<Vector, Double>> models) {
                return null;
            }

            @Override
            protected boolean shouldStop(int iterationsCnt, List<Model<Vector, Double>> models, Void meta) {
                return iterationsCnt >= 1;
            }
        };

        ModelsComposition mdl = trainer.fit(
            cacheMock,
            parts,
            (k, v) -> VectorUtils.of(v[0]),
            (k, v) -> v[1]);

        TestUtils.assertEquals(
            twoLinearlySeparableClasses.length * subsampleSize,
            mdl.apply(null),
            twoLinearlySeparableClasses.length / 10);
    }

    @Test
    public void testNaiveBaggingContextCount() {
        count((ctxCount, countData, integer) -> ctxCount);
    }

    @Test
    public void testNaiveBaggingDataCount() {
        count((ctxCount, countData, integer) -> countData.cnt);
    }

    protected void count(IgniteTriFunction<Long, CountData, Integer, Long> counter) {
        Map<Integer, Double[]> cacheMock = getCacheMock();

        CountTrainer countTrainer = new CountTrainer(counter);

        double subsampleSize = 0.3;

        ModelsComposition model = countTrainer
            .transform(TrainerTransformers.makeBagged(100, subsampleSize, new MeanValuePredictionsAggregator()))
            .fit(cacheMock, parts, null, null);

        Double res = model.apply(null);

        TestUtils.assertEquals(twoLinearlySeparableClasses.length * subsampleSize, res, twoLinearlySeparableClasses.length / 10);
    }

    @NotNull
    private Map<Integer, Double[]> getCacheMock() {
        Map<Integer, Double[]> cacheMock = new HashMap<>();

        for (int i = 0; i < twoLinearlySeparableClasses.length; i++) {
            double[] row = twoLinearlySeparableClasses[i];
            Double[] convertedRow = new Double[row.length];
            for (int j = 0; j < row.length; j++)
                convertedRow[j] = row[j];
            cacheMock.put(i, convertedRow);
        }
        return cacheMock;
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

    protected static class ConstModel implements Model<Vector, Double> {
        private double conzt;

        public ConstModel(double conzt) {
            this.conzt = conzt;
        }

        public double getConzt() {
            return conzt;
        }

        public void setConzt(double conzt) {
            this.conzt = conzt;
        }

        @Override
        public Double apply(Vector vector) {
            return conzt;
        }
    }
}
