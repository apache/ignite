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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapper;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.util.Utils;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract trainer implementing bagging logic. In each learning iteration the algorithm trains one model on subset of
 * learning sample and subspace of features space. Each model is produced from same model-class [e.g. Decision Trees].
 */
public abstract class BaggingModelTrainer extends DatasetTrainer<ModelsComposition, Double> {
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

        this.predictionsAggregator = predictionsAggregator;
        this.maximumFeaturesCntPerMdl = maximumFeaturesCntPerMdl;
        this.ensembleSize = ensembleSize;
        this.samplePartSizePerMdl = samplePartSizePerMdl;
        this.featureVectorSize = featureVectorSize;
    }

    /** {@inheritDoc} */
    @Override public <K, V> ModelsComposition fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        MLLogger log = environment.logger(getClass());
        log.log(MLLogger.VerboseLevel.LOW, "Start learning");

        Long startTs = System.currentTimeMillis();

        List<IgniteSupplier<ModelOnFeaturesSubspace>> tasks = new ArrayList<>();
        for(int i = 0; i < ensembleSize; i++)
            tasks.add(() -> learnModel(datasetBuilder, featureExtractor, lbExtractor));

        List<Model<Vector, Double>> models = environment.parallelismStrategy().submit(tasks)
            .stream().map(Promise::unsafeGet)
            .collect(Collectors.toList());

        double learningTime = (double)(System.currentTimeMillis() - startTs) / 1000.0;
        log.log(MLLogger.VerboseLevel.LOW, "The training time was %.2fs", learningTime);
        log.log(MLLogger.VerboseLevel.LOW, "Learning finished");
        return new ModelsComposition(models, predictionsAggregator);
    }

    /**
     * Trains one model on part of sample and features subspace.
     *
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     */
    @NotNull private <K, V> ModelOnFeaturesSubspace learnModel(
        DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        Random rnd = new Random();
        SHA256UniformMapper<K, V> sampleFilter = new SHA256UniformMapper<>(rnd);
        long featureExtractorSeed = rnd.nextLong();
        Map<Integer, Integer> featuresMapping = createFeaturesMapping(featureExtractorSeed, featureVectorSize);

        //TODO: IGNITE-8867 Need to implement bootstrapping algorithm
        Long startTs = System.currentTimeMillis();
        Model<Vector, Double> mdl = buildDatasetTrainerForModel().fit(
            datasetBuilder.withFilter((features, answer) -> sampleFilter.map(features, answer) < samplePartSizePerMdl),
            wrapFeatureExtractor(featureExtractor, featuresMapping),
            lbExtractor);
        double learningTime = (double)(System.currentTimeMillis() - startTs) / 1000.0;
        environment.logger(getClass()).log(MLLogger.VerboseLevel.HIGH, "One model training time was %.2fs", learningTime);

        return new ModelOnFeaturesSubspace(featuresMapping, mdl);
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
    protected abstract DatasetTrainer<? extends Model<Vector, Double>, Double> buildDatasetTrainerForModel();

    /**
     * Wraps the original feature extractor with features subspace mapping applying.
     *
     * @param featureExtractor Feature extractor.
     * @param featureMapping Feature mapping.
     */
    private <K, V> IgniteBiFunction<K, V, Vector> wrapFeatureExtractor(
        IgniteBiFunction<K, V, Vector> featureExtractor,
        Map<Integer, Integer> featureMapping) {

        return featureExtractor.andThen((IgniteFunction<Vector, Vector>)featureValues -> {
            double[] newFeaturesValues = new double[featureMapping.size()];
            featureMapping.forEach((localId, featureValueId) -> newFeaturesValues[localId] = featureValues.get(featureValueId));
            return VectorUtils.of(newFeaturesValues);
        });
    }
}
