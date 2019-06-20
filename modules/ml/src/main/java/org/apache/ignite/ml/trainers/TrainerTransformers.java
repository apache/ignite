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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.bagging.BaggedTrainer;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.trainers.transformers.BaggingUpstreamTransformer;
import org.apache.ignite.ml.util.Utils;

/**
 * Class containing various trainer transformers.
 */
public class TrainerTransformers {
    /**
     * Add bagging logic to a given trainer. No features bootstrapping is done.
     *
     * @param ensembleSize Size of ensemble.
     * @param subsampleRatio Subsample ratio to whole dataset.
     * @param aggregator Aggregator.
     * @param <L> Type of labels.
     * @return Bagged trainer.
     */
    public static <L> BaggedTrainer<L> makeBagged(
        DatasetTrainer<? extends IgniteModel, L> trainer,
        int ensembleSize,
        double subsampleRatio,
        PredictionsAggregator aggregator) {
        return makeBagged(trainer, ensembleSize, subsampleRatio, -1, -1, aggregator);
    }

    /**
     * Add bagging logic to a given trainer.
     *
     * @param ensembleSize Size of ensemble.
     * @param subsampleRatio Subsample ratio to whole dataset.
     * @param aggregator Aggregator.
     * @param featureVectorSize Feature vector dimensionality.
     * @param featuresSubspaceDim Feature subspace dimensionality.
     * @param <M> Type of one model in ensemble.
     * @param <L> Type of labels.
     * @return Bagged trainer.
     */
    public static <M extends IgniteModel<Vector, Double>, L> BaggedTrainer<L> makeBagged(
        DatasetTrainer<M, L> trainer,
        int ensembleSize,
        double subsampleRatio,
        int featureVectorSize,
        int featuresSubspaceDim,
        PredictionsAggregator aggregator) {
        return new BaggedTrainer<>(trainer,
            aggregator,
            ensembleSize,
            subsampleRatio,
            featureVectorSize,
            featuresSubspaceDim);
    }

    /**
     * This method accepts function which for given dataset builder and index of model in ensemble generates
     * task of training this model.
     *
     * @param trainingTaskGenerator Training test generator.
     * @param datasetBuilder Dataset builder.
     * @param ensembleSize Size of ensemble.
     * @param subsampleRatio Ratio (subsample size) / (initial dataset size).
     * @param featuresVectorSize Dimensionality of feature vector.
     * @param featureSubspaceDim Dimensionality of feature subspace.
     * @param aggregator Aggregator of models.
     * @param environment Environment.
     * @param <K> Type of keys in dataset builder.
     * @param <V> Type of values in dataset builder.
     * @param <M> Type of model.
     * @return Composition of models trained on bagged dataset.
     */
    private static <K, V, M extends IgniteModel<Vector, Double>> ModelsComposition runOnEnsemble(
        IgniteTriFunction<DatasetBuilder<K, V>, Integer, IgniteBiFunction<K, V, Vector>, IgniteSupplier<M>> trainingTaskGenerator,
        DatasetBuilder<K, V> datasetBuilder,
        int ensembleSize,
        double subsampleRatio,
        int featuresVectorSize,
        int featureSubspaceDim,
        IgniteBiFunction<K, V, Vector> extractor,
        PredictionsAggregator aggregator,
        LearningEnvironment environment) {

        MLLogger log = environment.logger(datasetBuilder.getClass());
        log.log(MLLogger.VerboseLevel.LOW, "Start learning.");

        List<int[]> mappings = null;
        if (featuresVectorSize > 0 && featureSubspaceDim != featuresVectorSize) {
            mappings = IntStream.range(0, ensembleSize).mapToObj(
                modelIdx -> getMapping(
                    featuresVectorSize,
                    featureSubspaceDim,
                    environment.randomNumbersGenerator().nextLong() + modelIdx))
                .collect(Collectors.toList());
        }

        Long startTs = System.currentTimeMillis();

        List<IgniteSupplier<M>> tasks = new ArrayList<>();
        List<IgniteBiFunction<K, V, Vector>> extractors = new ArrayList<>();
        if (mappings != null) {
            for (int[] mapping : mappings)
                extractors.add(wrapExtractor(extractor, mapping));
        }

        for (int i = 0; i < ensembleSize; i++) {
            DatasetBuilder<K, V> newBuilder =
                datasetBuilder.withUpstreamTransformer(BaggingUpstreamTransformer.builder(subsampleRatio, i));
            tasks.add(
                trainingTaskGenerator.apply(newBuilder, i, mappings != null ? extractors.get(i) : extractor));
        }

        List<ModelWithMapping<Vector, Double, M>> models = environment.parallelismStrategy().submit(tasks)
            .stream()
            .map(Promise::unsafeGet)
            .map(ModelWithMapping<Vector, Double, M>::new)
            .collect(Collectors.toList());

        // If we need to do projection, do it.
        if (mappings != null) {
            for (int i = 0; i < models.size(); i++)
                models.get(i).setMapping(VectorUtils.getProjector(mappings.get(i)));
        }

        double learningTime = (double)(System.currentTimeMillis() - startTs) / 1000.0;
        log.log(MLLogger.VerboseLevel.LOW, "The training time was %.2fs.", learningTime);
        log.log(MLLogger.VerboseLevel.LOW, "Learning finished.");

        return new ModelsComposition(models, aggregator);
    }

    /**
     * Get mapping R^featuresVectorSize -> R^maximumFeaturesCntPerMdl.
     *
     * @param featuresVectorSize Features vector size (Dimension of initial space).
     * @param maximumFeaturesCntPerMdl Dimension of target space.
     * @param seed Seed.
     * @return Mapping R^featuresVectorSize -> R^maximumFeaturesCntPerMdl.
     */
    public static int[] getMapping(int featuresVectorSize, int maximumFeaturesCntPerMdl, long seed) {
        return Utils.selectKDistinct(featuresVectorSize, maximumFeaturesCntPerMdl, new Random(seed));
    }

    /**
     * Creates feature extractor which is a composition of given feature extractor and projection given by
     * coordinate indexes mapping.
     *
     * @param featureExtractor Initial feature extractor.
     * @param featureMapping Coordinate indexes mapping.
     * @param <K> Type of keys.
     * @param <V> Type of values.
     * @return Composition of given feature extractor and projection given by coordinate indexes mapping.
     */
    private static <K, V> IgniteBiFunction<K, V, Vector> wrapExtractor(IgniteBiFunction<K, V, Vector> featureExtractor,
        int[] featureMapping) {
        return featureExtractor.andThen((IgniteFunction<Vector, Vector>)featureValues -> {
            double[] newFeaturesValues = new double[featureMapping.length];
            for (int i = 0; i < featureMapping.length; i++)
                newFeaturesValues[i] = featureValues.get(featureMapping[i]);

            return VectorUtils.of(newFeaturesValues);
        });
    }

    /**
     * Model with mapping from X to X.
     *
     * @param <X> Input space.
     * @param <Y> Output space.
     * @param <M> Model.
     */
    private static class ModelWithMapping<X, Y, M extends IgniteModel<X, Y>> implements IgniteModel<X, Y> {
        /** Model. */
        private final M model;

        /** Mapping. */
        private IgniteFunction<X, X> mapping;

        /**
         * Create instance of this class from a given model.
         * Identity mapping will be used as a mapping.
         *
         * @param model Model.
         */
        public ModelWithMapping(M model) {
            this(model, x -> x);
        }

        /**
         * Create instance of this class from given model and mapping.
         *
         * @param model Model.
         * @param mapping Mapping.
         */
        public ModelWithMapping(M model, IgniteFunction<X, X> mapping) {
            this.model = model;
            this.mapping = mapping;
        }

        /**
         * Sets mapping.
         *
         * @param mapping Mapping.
         */
        public void setMapping(IgniteFunction<X, X> mapping) {
            this.mapping = mapping;
        }

        /** {@inheritDoc} */
        @Override public Y predict(X x) {
            return model.predict(mapping.apply(x));
        }

        /**
         * Gets model.
         *
         * @return Model.
         */
        public M model() {
            return model;
        }

        /**
         * Gets mapping.
         *
         * @return Mapping.
         */
        public IgniteFunction<X, X> mapping() {
            return mapping;
        }
    }
}
