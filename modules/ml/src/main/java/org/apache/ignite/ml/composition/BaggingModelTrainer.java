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

package org.apache.ignite.ml.composition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapper;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.util.Utils;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract trainer implementing bagging logic.
 */
public abstract class BaggingModelTrainer implements DatasetTrainer<ModelsComposition, Double> {
    /**
     * Predictions aggregator.
     */
    private final PredictionsAggregator predictionsAggregator;
    /**
     * Number of features to draw from original features vector to train each model.
     */
    private final int maximumFeaturesCntPerMdl;
    /**
     * Ensemble size.
     */
    private final int ensembleSize;
    /**
     * Size of sample part in percent to train one model.
     */
    private final double samplePartSizePerMdl;
    /**
     * Feature vector size.
     */
    private final int featureVectorSize;
    /**
     * Learning thread pool.
     */
    private final ExecutorService threadPool;

    /**
     * Constructs new instance of BaggingModelTrainer.
     *
     * @param predictionsAggregator Predictions aggregator.
     * @param featureVectorSize Feature vector size.
     * @param maximumFeaturesCntPerMdl Number of features to draw from original features vector to train each model.
     * @param ensembleSize Ensemble size.
     * @param samplePartSizePerMdl Size of sample part in percent to train one model.
     */
    public BaggingModelTrainer(PredictionsAggregator predictionsAggregator,
        int featureVectorSize,
        int maximumFeaturesCntPerMdl,
        int ensembleSize,
        double samplePartSizePerMdl) {

        this(predictionsAggregator, featureVectorSize, maximumFeaturesCntPerMdl, ensembleSize,
            samplePartSizePerMdl, null);
    }

    /**
     * Constructs new instance of BaggingModelTrainer.
     *
     * @param predictionsAggregator Predictions aggregator.
     * @param featureVectorSize Feature vector size.
     * @param maximumFeaturesCntPerMdl Number of features to draw from original features vector to train each model.
     * @param ensembleSize Ensemble size.
     * @param samplePartSizePerMdl Size of sample part in percent to train one model.
     * @param threadPool Learning thread pool.
     */
    public BaggingModelTrainer(PredictionsAggregator predictionsAggregator,
        int featureVectorSize,
        int maximumFeaturesCntPerMdl,
        int ensembleSize,
        double samplePartSizePerMdl,
        ExecutorService threadPool) {

        this.predictionsAggregator = predictionsAggregator;
        this.maximumFeaturesCntPerMdl = maximumFeaturesCntPerMdl;
        this.ensembleSize = ensembleSize;
        this.samplePartSizePerMdl = samplePartSizePerMdl;
        this.featureVectorSize = featureVectorSize;
        this.threadPool = threadPool;
    }

    /** {@inheritDoc} */
    @Override public <K, V> ModelsComposition fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        List<ModelsComposition.ModelOnFeaturesSubspace> learnedModels = new ArrayList<>();
        List<Future<ModelsComposition.ModelOnFeaturesSubspace>> futures = new ArrayList<>();

        for (int i = 0; i < ensembleSize; i++) {
            if (threadPool == null)
                learnedModels.add(learnModel(datasetBuilder, featureExtractor, lbExtractor));
            else {
                Future<ModelsComposition.ModelOnFeaturesSubspace> fut = threadPool.submit(() -> {
                    return learnModel(datasetBuilder, featureExtractor, lbExtractor);
                });

                futures.add(fut);
            }
        }

        if (threadPool != null) {
            for (Future<ModelsComposition.ModelOnFeaturesSubspace> future : futures) {
                try {
                    learnedModels.add(future.get());
                }
                catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return new ModelsComposition(learnedModels, predictionsAggregator);
    }

    /**
     * Trains one model on part of sample and features subspace.
     *
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     */
    @NotNull private <K, V> ModelsComposition.ModelOnFeaturesSubspace learnModel(
        DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        Random rnd = new Random();
        SHA256UniformMapper<K, V> sampleFilter = new SHA256UniformMapper<>(rnd);
        long featureExtractorSeed = rnd.nextLong();
        Map<Integer, Integer> featuresMapping = createFeaturesMapping(featureExtractorSeed, featureVectorSize);

        //TODO: IGNITE-8867 Need to implement bootstrapping algorithm
        Model<double[], Double> mdl = buildDatasetTrainerForModel().fit(
            datasetBuilder.withFilter((features, answer) -> sampleFilter.map(features, answer) < samplePartSizePerMdl),
            wrapFeatureExtractor(featureExtractor, featuresMapping),
            lbExtractor);

        return new ModelsComposition.ModelOnFeaturesSubspace(featuresMapping, mdl);
    }

    /**
     * Constructs mapping from original feature vector to subspace.
     *
     * @param seed Seed.
     * @param featuresVectorSize Features vector size.
     */
    private Map<Integer, Integer> createFeaturesMapping(long seed, int featuresVectorSize) {
        int[] featureIdxs = Utils.selectKDistinct(featuresVectorSize, maximumFeaturesCntPerMdl, new Random(seed));
        Map<Integer, Integer> locFeaturesMapping = new HashMap<>();

        IntStream.range(0, maximumFeaturesCntPerMdl)
            .forEach(localId -> locFeaturesMapping.put(localId, featureIdxs[localId]));

        return locFeaturesMapping;
    }

    /**
     * Creates trainer specific to ensemble.
     */
    protected abstract DatasetTrainer<? extends Model<double[], Double>, Double> buildDatasetTrainerForModel();

    /**
     * Wraps the original feature extractor with features subspace mapping applying.
     *
     * @param featureExtractor Feature extractor.
     * @param featureMapping Feature mapping.
     */
    private <K, V> IgniteBiFunction<K, V, double[]> wrapFeatureExtractor(
        IgniteBiFunction<K, V, double[]> featureExtractor,
        Map<Integer, Integer> featureMapping) {

        return featureExtractor.andThen((IgniteFunction<double[], double[]>)featureValues -> {
            double[] newFeaturesValues = new double[featureMapping.size()];
            featureMapping.forEach((localId, featureValueId) -> newFeaturesValues[localId] = featureValues[featureValueId]);
            return newFeaturesValues;
        });
    }
}
