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
import org.apache.ignite.ml.composition.answercomputer.ModelsCompositionAnswerComputer;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapper;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.util.Utils;

import java.util.*;
import java.util.stream.IntStream;

public abstract class BaggingModelTrainer<M extends Model<double[], Double>> implements DatasetTrainer<ModelsComposition<M>, Double> {
    private final ModelsCompositionAnswerComputer modelsCompositionAnswerComputer;
    private final int maximumFeaturesCountPerModel;
    private final int countOfModels;
    private final double samplePartSizePerModel;
    private final int featureVectorSize;

    public BaggingModelTrainer(ModelsCompositionAnswerComputer modelsCompositionAnswerComputer,
                               int featureVectorSize,
                               int maximumFeaturesCountPerModel, int countOfModels,
                               double samplePartSizePerModel) {
        this.modelsCompositionAnswerComputer = modelsCompositionAnswerComputer;
        this.maximumFeaturesCountPerModel = maximumFeaturesCountPerModel;
        this.countOfModels = countOfModels;
        this.samplePartSizePerModel = samplePartSizePerModel;
        this.featureVectorSize = featureVectorSize;
    }

    @Override
    public <K, V> ModelsComposition<M> fit(DatasetBuilder<K, V> datasetBuilder,
                                           IgniteBiFunction<K, V, double[]> featureExtractor,
                                           IgniteBiFunction<K, V, Double> lbExtractor) {
        List<ModelsComposition.ModelOnFeaturesSubspace<M>> learnedModels = new ArrayList<>();

        for(int i = 0; i < countOfModels; i++) {
            final Random rnd = new Random();
            final SHA256UniformMapper<K, V> sampleFilter = new SHA256UniformMapper<>(rnd);
            final long featureExtractorSeed = rnd.nextLong();
            Map<Integer, Integer> featuresMapping = createFeaturesMapping(featureExtractorSeed, featureVectorSize);

            M model = buildDatasetTrainerForModel().fit(
                    datasetBuilder.withFilter((features, answer) -> sampleFilter.map(features, answer) < samplePartSizePerModel),
                    wrapFeatureExtractor(featureExtractor, featuresMapping),
                    lbExtractor);

            learnedModels.add(new ModelsComposition.ModelOnFeaturesSubspace<>(featuresMapping, model));
        }

        return new ModelsComposition<>(modelsCompositionAnswerComputer, learnedModels);
    }

    private Map<Integer, Integer> createFeaturesMapping(long featureExtractorSeed, int featuresVectorSize) {
        final int[] featureIdxs = Utils.selectKDistinct(featuresVectorSize,
                maximumFeaturesCountPerModel,
                new Random(featureExtractorSeed)
        );
        final Map<Integer, Integer> localFeaturesMapping = new HashMap<>();
        IntStream.range(0, maximumFeaturesCountPerModel)
                .forEach(localId -> localFeaturesMapping.put(localId, featureIdxs[localId]));

        return localFeaturesMapping;
    }

    protected abstract DatasetTrainer<M, Double> buildDatasetTrainerForModel();

    private <K, V> IgniteBiFunction<K, V, double[]> wrapFeatureExtractor(IgniteBiFunction<K, V, double[]> featureExtractor, Map<Integer, Integer> featureMapping) {
        return featureExtractor.andThen((IgniteFunction<double[], double[]>) featureValues -> {
            double[] newFeaturesValues = new double[featureMapping.size()];
            featureMapping.forEach((localId, featureValueId) -> newFeaturesValues[localId] = featureValues[featureValueId]);
            return newFeaturesValues;
        });
    }
}
