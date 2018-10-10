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

package org.apache.ignite.ml.trainers;

import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TrainerTransformers {
    public static <M extends Model<Vector, Double>, L> IgniteFunction<DatasetTrainer<M, L>, DatasetTrainer<ModelsComposition, L>> makeBagged(
        int ensembleSize,
        double subsampleSize,
        PredictionsAggregator aggregator) {
        return trainer -> new DatasetTrainer<ModelsComposition, L>() {
            @Override
            public <K, V> ModelsComposition fit(
                DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor,
                IgniteBiFunction<K, V, L> lbExtractor) {
                return runOnEnsemble(
                    (db, i) -> (() -> trainer.fit(db, featureExtractor, lbExtractor)),
                    datasetBuilder,
                    ensembleSize,
                    subsampleSize,
                    aggregator,
                    environment);
            }

            @Override
            protected boolean checkState(ModelsComposition mdl) {
                return mdl.getModels().stream().allMatch(m -> trainer.checkState((M) m));
            }

            @Override
            protected <K, V> ModelsComposition updateModel(
                ModelsComposition mdl,
                DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor,
                IgniteBiFunction<K, V, L> lbExtractor) {
                return runOnEnsemble(
                    (db, i) -> (() -> trainer.updateModel((M) mdl.getModels().get(i), db, featureExtractor, lbExtractor)),
                    datasetBuilder,
                    ensembleSize,
                    subsampleSize,
                    aggregator,
                    environment);
            }
        };
    }

    private static <X, Y, M extends Model<Vector, Double>> ModelsComposition runOnEnsemble(
        IgniteBiFunction<DatasetBuilder<X, Y>, Integer, IgniteSupplier<M>> supplierGenerator,
        DatasetBuilder<X, Y> datasetBuilder,
        int ensembleSize,
        double subsampleSize,
        PredictionsAggregator aggregator,
        LearningEnvironment environment) {
        MLLogger log = environment.logger(datasetBuilder.getClass());
        log.log(MLLogger.VerboseLevel.LOW, "Start learning");

        Long startTs = System.currentTimeMillis();

        DatasetBuilder<X, Y> bootstrappedBuilder = datasetBuilder.addStreamTransformer(
            (s, rnd) -> s.flatMap(en -> sampleForBagging(en, rnd)),
            () -> new PoissonDistribution(subsampleSize)
        );

        List<IgniteSupplier<M>> tasks = new ArrayList<>();

        for (int i = 0; i < ensembleSize; i++) {
            tasks.add(supplierGenerator.apply(bootstrappedBuilder, i));
        }

        List<M> models = environment.parallelismStrategy().submit(tasks)
            .stream().map(Promise::unsafeGet)
            .collect(Collectors.toList());

        double learningTime = (double) (System.currentTimeMillis() - startTs) / 1000.0;
        log.log(MLLogger.VerboseLevel.LOW, "The training time was %.2fs", learningTime);
        log.log(MLLogger.VerboseLevel.LOW, "Learning finished");

        return new ModelsComposition(models, aggregator);
    }

    private static <K, V> Stream<UpstreamEntry<K, V>> sampleForBagging(
        UpstreamEntry<K, V> en,
        PoissonDistribution poissonDistribution) {
        int count = poissonDistribution.sample();

        return IntStream.range(0, count).mapToObj(i -> en);
    }
}
