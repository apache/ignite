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
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Class containing various trainer transformers.
 */
public class TrainerTransformers {
    /**
     * Add bagging logic to a given trainer.
     *
     * @param ensembleSize   Size of ensemble.
     * @param subsampleRatio Subsample ratio to whole dataset.
     * @param aggregator     Aggregator.
     * @param <M>            Type of one model in ensemble.
     * @param <L>            Type of labels.
     * @return Bagged trainer.
     */
    public static <M extends Model<Vector, Double>, L> DatasetTrainer<ModelsComposition, L> makeBagged(
        DatasetTrainer<M, L> trainer,
        int ensembleSize,
        double subsampleRatio,
        PredictionsAggregator aggregator) {
        return makeBagged(trainer, ensembleSize, subsampleRatio, -1, -1, aggregator);
    }

    /**
     * Add bagging logic to a given trainer.
     *
     * @param ensembleSize   Size of ensemble.
     * @param subsampleRatio Subsample ratio to whole dataset.
     * @param aggregator     Aggregator.
     * @param <M>            Type of one model in ensemble.
     * @param <L>            Type of labels.
     * @return Bagged trainer.
     */
    public static <M extends Model<Vector, Double>, L> DatasetTrainer<ModelsComposition, L> makeBagged(
        DatasetTrainer<M, L> trainer,
        int ensembleSize,
        double subsampleRatio,
        int featureVectorSize,
        int maxFeaturesCntPerMdl,
        PredictionsAggregator aggregator) {
        return new DatasetTrainer<ModelsComposition, L>() {
            @Override
            public <K, V> ModelsComposition fit(
                DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor,
                IgniteBiFunction<K, V, L> lbExtractor) {
                return runOnEnsemble(
                    (db, i, fe) -> (() -> trainer.fit(db, fe, lbExtractor)),
                    datasetBuilder,
                    ensembleSize,
                    subsampleRatio,
                    featureVectorSize,
                    maxFeaturesCntPerMdl,
                    featureExtractor,
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
                    (db, i, fe) -> (() -> trainer.updateModel(((ModelWithEndomorphism<Vector, Double, M>) mdl.getModels().get(i)).model(), db, fe, lbExtractor)),
                    datasetBuilder,
                    ensembleSize,
                    subsampleRatio,
                    featureVectorSize,
                    maxFeaturesCntPerMdl,
                    featureExtractor,
                    aggregator,
                    environment);
            }
        };
    }

    /**
     * This method accepts function which for given dataset builder and index of model in ensemble generates
     * task of training this model.
     *
     * @param trainingTaskGenerator Training test generator.
     * @param datasetBuilder        Dataset builder.
     * @param ensembleSize          Size of ensemble.
     * @param subsampleRatio        Ratio (subsample size) / (initial dataset size).
     * @param aggregator            Aggregator of models.
     * @param environment           Environment.
     * @param <K>                   Type of keys in dataset builder.
     * @param <V>                   Type of values in dataset builder.
     * @param <M>                   Type of model.
     * @return Composition of models trained on bagged dataset.
     */
    private static <K, V, M extends Model<Vector, Double>> ModelsComposition runOnEnsemble(
        IgniteTriFunction<DatasetBuilder<K, V>, Integer, IgniteBiFunction<K, V, Vector>, IgniteSupplier<M>> trainingTaskGenerator,
        DatasetBuilder<K, V> datasetBuilder,
        int ensembleSize,
        double subsampleRatio,
        int featuresVectorSize,
        int maximumFeaturesCntPerMdl,
        IgniteBiFunction<K, V, Vector> extractor,
        PredictionsAggregator aggregator,
        LearningEnvironment environment) {
        MLLogger log = environment.logger(datasetBuilder.getClass());
        log.log(MLLogger.VerboseLevel.LOW, "Start learning.");

        List<Map<Integer, Integer>> mappings = null;
        if (featuresVectorSize > 0) {
            mappings = IntStream.range(0, ensembleSize).mapToObj(modelIdx -> getMapping(featuresVectorSize, maximumFeaturesCntPerMdl))
                .collect(Collectors.toList());
        }

        Long startTs = System.currentTimeMillis();

        DatasetBuilder<K, V> bootstrappedBuilder = datasetBuilder.addStreamTransformer(
            // Sequentiality of stream here is needed because we use instance of
            // RNG as data, to make it deterministic we should fix order.
            (s, rnd) -> s.sequential().flatMap(en -> repeatEntry(en, rnd)),
            () -> new PoissonDistribution(subsampleRatio)
        );

        List<IgniteSupplier<M>> tasks = new ArrayList<>();
        List<IgniteBiFunction<K, V, Vector>> extractors = null;
        if (mappings != null) {
            extractors = mappings
                .stream()
                .map(m -> wrapExtractor(extractor, m))
                .collect(Collectors.toList());
        }

        for (int i = 0; i < ensembleSize; i++) {
            tasks.add(trainingTaskGenerator.apply(bootstrappedBuilder, i, mappings != null ? extractors.get(i) : extractor));
        }

        List<ModelWithEndomorphism<Vector, Double, M>> models = environment.parallelismStrategy().submit(tasks)
            .stream()
            .map(Promise::unsafeGet)
            .map(ModelWithEndomorphism<Vector, Double, M>::new)
            .collect(Collectors.toList());

        // If we need to do projection, do it.
        if (mappings != null) {
            for (int i = 0; i < models.size(); i++) {
                models.get(i).setEndo(getProjector(mappings.get(i)));
            }
        }

        double learningTime = (double) (System.currentTimeMillis() - startTs) / 1000.0;
        log.log(MLLogger.VerboseLevel.LOW, "The training time was %.2fs.", learningTime);
        log.log(MLLogger.VerboseLevel.LOW, "Learning finished.");

        return new ModelsComposition(models, aggregator);
    }

    @NotNull
    public static Map<Integer, Integer> getMapping(int featuresVectorSize, int maximumFeaturesCntPerMdl) {
        int[] featureIdxs = Utils.selectKDistinct(featuresVectorSize, maximumFeaturesCntPerMdl, new Random());
        Map<Integer, Integer> featureMapping = new HashMap<>();
        IntStream.range(0, maximumFeaturesCntPerMdl)
            .forEach(localId -> featureMapping.put(localId, featureIdxs[localId]));
        return featureMapping;
    }

    /**
     * Repeats each entry count of times distributed according given Poisson distribution.
     *
     * @param en                  Upstream entry.
     * @param poissonDistribution Poisson distribution.
     * @param <K>                 Type of keys of upstream data.
     * @param <V>                 Type of values of upstream data.
     * @return Stream containing repeating upstream entry.
     */
    private static <K, V> Stream<UpstreamEntry<K, V>> repeatEntry(
        UpstreamEntry<K, V> en,
        PoissonDistribution poissonDistribution) {
        int count = poissonDistribution.sample();

        return IntStream.range(0, count).mapToObj(i -> en);
    }

    public static IgniteFunction<Vector, Vector> getProjector(Map<Integer, Integer> mapping) {
        return v -> {
            Vector res = VectorUtils.zeroes(mapping.size());
            mapping.keySet().stream().forEach(locId -> res.set(locId, v.get(mapping.get(locId))));

            return res;
        };
    }

    private static <K, V> IgniteBiFunction<K, V, Vector> wrapExtractor(IgniteBiFunction<K, V, Vector> featureExtractor, Map<Integer, Integer> featureMapping) {
        return featureExtractor.andThen((IgniteFunction<Vector, Vector>) featureValues -> {
            double[] newFeaturesValues = new double[featureMapping.size()];
            featureMapping.forEach((localId, featureValueId) -> newFeaturesValues[localId] = featureValues.get(featureValueId));
            return VectorUtils.of(newFeaturesValues);
        });
    }

    /**
     * Model with endomorphism (Function from X to X).
     *
     * @param <X> Input space.
     * @param <Y> Output space.
     * @param <M> Model.
     */
    private static class ModelWithEndomorphism<X, Y, M extends Model<X, Y>> implements Model<X, Y> {
        M model;
        IgniteFunction<X, X> endo;

        public ModelWithEndomorphism(M model) {
            this(model, x -> x);
        }

        public ModelWithEndomorphism(M model, IgniteFunction<X, X> endo) {
            this.model = model;
            this.endo = endo;
        }

        public void setEndo(IgniteFunction<X, X> endo) {
            this.endo = endo;
        }

        @Override
        public Y apply(X x) {
            return model.apply(endo.apply(x));
        }

        public M model() {
            return model;
        }

        public IgniteFunction<X, X> endo() {
            return endo;
        }
    }
}
