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
import org.apache.ignite.ml.composition.predictionsaggregator.WeightedPredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.knn.regression.KNNRegressionTrainer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionSGDTrainer;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.data.DecisionTreeDataBuilder;
import org.apache.ignite.ml.tree.randomforest.RandomForestRegressionTrainer;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract Gradient Boosting trainer. It implements gradient descent in functional space using user-selected regressor
 * in child class. Each learning iteration the trainer evaluate gradient of error-function and fit regression model to
 * it. After learning step the model is used in models composition of regressions with weight equal to gradient descent
 * step.
 *
 * These classes can be used as regressor trainers: {@link DecisionTreeRegressionTrainer}, {@link KNNRegressionTrainer},
 * {@link LinearRegressionLSQRTrainer}, {@link RandomForestRegressionTrainer}, {@link LinearRegressionSGDTrainer}.
 *
 * But in practice Decision Trees is most used regressors (see: {@link DecisionTreeRegressionTrainer}).
 */
abstract class GDBTrainer extends DatasetTrainer<Model<Vector, Double>, Double> {
    /** Gradient step. */
    private final double gradientStep;
    /** Count of iterations. */
    private final int cntOfIterations;
    /**
     * Gradient of loss function. First argument is sample size, second argument is valid answer, third argument is
     * current model prediction.
     */
    private final IgniteTriFunction<Long, Double, Double, Double> lossGradient;

    /**
     * Constructs GDBTrainer instance.
     *
     * @param gradStepSize Grad step size.
     * @param cntOfIterations Count of learning iterations.
     * @param lossGradient Gradient of loss function. First argument is sample size, second argument is valid answer
     * third argument is current model prediction.
     */
    public GDBTrainer(double gradStepSize, Integer cntOfIterations,
        IgniteTriFunction<Long, Double, Double, Double> lossGradient) {
        gradientStep = gradStepSize;
        this.cntOfIterations = cntOfIterations;
        this.lossGradient = lossGradient;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Model<Vector, Double> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        learnLabels(datasetBuilder, featureExtractor, lbExtractor);

        IgniteBiTuple<Double, Long> initAndSampleSize = computeInitialValue(datasetBuilder,
            featureExtractor, lbExtractor);
        Double mean = initAndSampleSize.get1();
        Long sampleSize = initAndSampleSize.get2();

        List<Model<Vector, Double>> models = new ArrayList<>();
        double[] compositionWeights = new double[cntOfIterations];
        Arrays.fill(compositionWeights, gradientStep);
        WeightedPredictionsAggregator resAggregator = new WeightedPredictionsAggregator(compositionWeights, mean);

        long learningStartTs = System.currentTimeMillis();
        for (int i = 0; i < cntOfIterations; i++) {
            double[] weights = Arrays.copyOf(compositionWeights, i);
            WeightedPredictionsAggregator aggregator = new WeightedPredictionsAggregator(weights, mean);
            Model<Vector, Double> currComposition = new ModelsComposition(models, aggregator);

            IgniteBiFunction<K, V, Double> lbExtractorWrap = (k, v) -> {
                Double realAnswer = externalLabelToInternal(lbExtractor.apply(k, v));
                Double mdlAnswer = currComposition.apply(featureExtractor.apply(k, v));
                return -lossGradient.apply(sampleSize, realAnswer, mdlAnswer);
            };

            long startTs = System.currentTimeMillis();
            models.add(buildBaseModelTrainer().fit(datasetBuilder, featureExtractor, lbExtractorWrap));
            double learningTime = (double)(System.currentTimeMillis() - startTs) / 1000.0;
            environment.logger(getClass()).log(MLLogger.VerboseLevel.LOW, "One model training time was %.2fs", learningTime);
        }
        double learningTime = (double)(System.currentTimeMillis() - learningStartTs) / 1000.0;
        environment.logger(getClass()).log(MLLogger.VerboseLevel.LOW, "The training time was %.2fs", learningTime);

        return new ModelsComposition(models, resAggregator) {
            @Override public Double apply(Vector features) {
                return internalLabelToExternal(super.apply(features));
            }
        };
    }

    /**
     * Defines unique labels in dataset if need (useful in case of classification).
     *
     * @param builder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lExtractor Labels extractor.
     */
    protected abstract <V, K> void learnLabels(DatasetBuilder<K, V> builder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lExtractor);

    /**
     * Returns regressor model trainer for one step of GDB.
     */
    @NotNull protected abstract DatasetTrainer<? extends Model<Vector, Double>, Double> buildBaseModelTrainer();

    /**
     * Maps external representation of label to internal.
     *
     * @param lbl Label value.
     */
    protected abstract double externalLabelToInternal(double lbl);

    /**
     * Maps internal representation of label to external.
     *
     * @param lbl Label value.
     */
    protected abstract double internalLabelToExternal(double lbl);

    /**
     * Compute mean value of label as first approximation.
     *
     * @param builder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     */
    protected <V, K> IgniteBiTuple<Double, Long> computeInitialValue(DatasetBuilder<K, V> builder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
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
}
