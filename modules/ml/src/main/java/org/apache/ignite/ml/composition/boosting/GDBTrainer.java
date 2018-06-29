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

package org.apache.ignite.ml.composition.boosting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.BoostingPredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.data.DecisionTreeDataBuilder;
import org.jetbrains.annotations.NotNull;

abstract class GDBTrainer implements DatasetTrainer<Model<Vector, Double>, Double> {
    private final double gradientStep;
    private final int countOfModels;

    public GDBTrainer(double gradStepSize, Integer modelsCnt) {
        gradientStep = gradStepSize;
        this.countOfModels = modelsCnt;
    }

    @Override public <K, V> Model<Vector, Double> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        learnLabels(datasetBuilder, featureExtractor, lbExtractor);

        IgniteBiTuple<Double, Long> initAndSampleSize = computeInitialValue(datasetBuilder,
            featureExtractor, lbExtractor);
        Double mean = initAndSampleSize.get1();
        Long sampleSize = initAndSampleSize.get2();

        List<Model<Vector, Double>> models = new ArrayList<>();
        double[] compositionWeights = new double[countOfModels];
        Arrays.fill(compositionWeights, gradientStep);
        BoostingPredictionsAggregator resAggregator = new BoostingPredictionsAggregator(mean, compositionWeights);

        for (int i = 0; i < countOfModels; i++) {
            double[] weights = Arrays.copyOf(compositionWeights, i);
            BoostingPredictionsAggregator aggregator = new BoostingPredictionsAggregator(mean, weights);
            Model<Vector, Double> currComposition = new ModelsComposition(models, aggregator);

            IgniteBiFunction<K, V, Double> lbExtractorWrap = (k, v) -> {
                Double realAnswer = externalLabelToInternal(lbExtractor.apply(k, v));
                Double mdlAnswer = currComposition.apply(Vector.of(featureExtractor.apply(k, v)));
                return -grad(sampleSize, realAnswer, mdlAnswer);
            };

            models.add(buildBaseModelTrainer().fit(datasetBuilder, featureExtractor, lbExtractorWrap));
        }

        return new ModelsComposition(models, resAggregator) {
            @Override public Double apply(Vector features) {
                return internalLabelToExternal(super.apply(features));
            }
        };
    }

    protected abstract  <V, K> void learnLabels(DatasetBuilder<K, V> builder,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lExtractor);

    @NotNull protected abstract DatasetTrainer<? extends Model<Vector, Double>, Double> buildBaseModelTrainer();

    protected abstract double externalLabelToInternal(double x);

    protected abstract double internalLabelToExternal(double x);

    protected <V, K> IgniteBiTuple<Double, Long> computeInitialValue(DatasetBuilder<K, V> builder,
        IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        try (Dataset<EmptyContext, DecisionTreeData> dataset = builder.build(
            new EmptyContextBuilder<>(),
            new DecisionTreeDataBuilder<>(featureExtractor, lbExtractor)
        )) {
            IgniteBiTuple<Double, Long> meanTuple = dataset.compute(
                data -> {
                    double sum = Arrays.stream(data.getLabels()).map(this::externalLabelToInternal).sum();
                    return new IgniteBiTuple<>(sum, (long)data.getLabels().length);
                },
                (a, b) -> {
                    if (a == null)
                        return b;
                    if (b == null)
                        return a;

                    a.set1(a.get1() + b.get1());
                    a.set2(a.get2() + b.get2());
                    return a;
                }
            );

            meanTuple.set1(meanTuple.get1() / meanTuple.get2());
            return meanTuple;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract double grad(long sampleSize, double answer, double prediction);
}
