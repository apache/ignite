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

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.answercomputer.PredictionsAggregator;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapper;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public abstract class BaggingModelTrainer<M extends Model<double[], Double>> implements DatasetTrainer<ModelsComposition<M>, Double> {
    private final PredictionsAggregator predictionsAggregator;
    private final int maximumFeaturesCntPerMdl;
    private final int cntOfModels;
    private final double samplePartSizePerMdl;
    private final int featureVectorSize;
    private final ExecutorService threadPool;

    public BaggingModelTrainer(PredictionsAggregator predictionsAggregator,
                               int featureVectorSize,
                               int maximumFeaturesCntPerMdl,
                               int cntOfModels,
                               double samplePartSizePerMdl) {
        this(predictionsAggregator, featureVectorSize, maximumFeaturesCntPerMdl, cntOfModels, samplePartSizePerMdl, null);
    }

    public BaggingModelTrainer(PredictionsAggregator predictionsAggregator,
                               int featureVectorSize,
                               int maximumFeaturesCntPerMdl,
                               int cntOfModels,
                               double samplePartSizePerMdl,
                               ExecutorService threadPool) {
        this.predictionsAggregator = predictionsAggregator;
        this.maximumFeaturesCntPerMdl = maximumFeaturesCntPerMdl;
        this.cntOfModels = cntOfModels;
        this.samplePartSizePerMdl = samplePartSizePerMdl;
        this.featureVectorSize = featureVectorSize;
        this.threadPool = threadPool;
    }

    @Override
    public <K, V> ModelsComposition<M> fit(DatasetBuilder<K, V> datasetBuilder,
                                           IgniteBiFunction<K, V, double[]> featureExtractor,
                                           IgniteBiFunction<K, V, Double> lbExtractor) {
        List<ModelsComposition.ModelOnFeaturesSubspace<M>> learnedModels = new ArrayList<>();

        List<Future<ModelsComposition.ModelOnFeaturesSubspace<M>>> futures = new ArrayList<>();
        for (int i = 0; i < cntOfModels; i++) {
            if (threadPool == null)
                learnedModels.add(buildModel(datasetBuilder, featureExtractor, lbExtractor));
            else {
                Future<ModelsComposition.ModelOnFeaturesSubspace<M>> future = threadPool.submit(() -> {
                    return buildModel(datasetBuilder, featureExtractor, lbExtractor);
                });

                futures.add(future);
            }
        }

        if (threadPool != null) {
            for (Future<ModelsComposition.ModelOnFeaturesSubspace<M>> future : futures) {
                try {
                    learnedModels.add(future.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return new ModelsComposition<>(learnedModels, predictionsAggregator);
    }

    @NotNull
    private <K, V> ModelsComposition.ModelOnFeaturesSubspace<M> buildModel(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {
        final Random rnd = new Random();
        final SHA256UniformMapper<K, V> sampleFilter = new SHA256UniformMapper<>(rnd);
        final long featureExtractorSeed = rnd.nextLong();
        Map<Integer, Integer> featuresMapping = createFeaturesMapping(featureExtractorSeed, featureVectorSize);

        M model = buildDatasetTrainerForModel().fit(
                datasetBuilder.withFilter((features, answer) -> sampleFilter.map(features, answer) < samplePartSizePerMdl),
                wrapFeatureExtractor(featureExtractor, featuresMapping),
                lbExtractor);

        return new ModelsComposition.ModelOnFeaturesSubspace<>(featuresMapping, model);
    }

    private Map<Integer, Integer> createFeaturesMapping(long featureExtractorSeed, int featuresVectorSize) {
        final int[] featureIdxs = Utils.selectKDistinct(featuresVectorSize,
                maximumFeaturesCntPerMdl,
                new Random(featureExtractorSeed)
        );
        final Map<Integer, Integer> locFeaturesMapping = new HashMap<>();
        IntStream.range(0, maximumFeaturesCntPerMdl)
                .forEach(localId -> locFeaturesMapping.put(localId, featureIdxs[localId]));

        return locFeaturesMapping;
    }

    protected abstract DatasetTrainer<M, Double> buildDatasetTrainerForModel();

    private <K, V> IgniteBiFunction<K, V, double[]> wrapFeatureExtractor(
            IgniteBiFunction<K, V, double[]> featureExtractor, Map<Integer, Integer> featureMapping) {
        return featureExtractor.andThen((IgniteFunction<double[], double[]>) featureValues -> {
            double[] newFeaturesValues = new double[featureMapping.size()];
            featureMapping.forEach((localId, featureValueId) -> newFeaturesValues[localId] = featureValues[featureValueId]);
            return newFeaturesValues;
        });
    }
}
