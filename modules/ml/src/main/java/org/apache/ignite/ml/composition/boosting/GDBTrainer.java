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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckerFactory;
import org.apache.ignite.ml.composition.boosting.convergence.mean.MeanAbsValueConvergenceCheckerFactory;
import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.composition.predictionsaggregator.WeightedPredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.knn.regression.KNNRegressionTrainer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
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
public abstract class GDBTrainer extends DatasetTrainer<GDBModel, Double> {
    /** Gradient step. */
    private final double gradientStep;

    /** Count of iterations. */
    private final int cntOfIterations;

    /**
     * Loss function.
     */
    protected final Loss loss;

    /** Check convergence strategy factory. */
    protected ConvergenceCheckerFactory checkConvergenceStgyFactory = new MeanAbsValueConvergenceCheckerFactory(0.001);

    /**
     * Constructs GDBTrainer instance.
     *
     * @param gradStepSize Grad step size.
     * @param cntOfIterations Count of learning iterations.
     * @param loss Gradient of loss function. First argument is sample size, second argument is valid answer third
     * argument is current model prediction.
     */
    public GDBTrainer(double gradStepSize, Integer cntOfIterations, Loss loss) {
        gradientStep = gradStepSize;
        this.cntOfIterations = cntOfIterations;
        this.loss = loss;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GDBModel fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder,
                                                  Preprocessor<K, V> preprocessor) {
        return updateModel(null, datasetBuilder, preprocessor);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> GDBModel updateModel(GDBModel mdl,
                                                             DatasetBuilder<K, V> datasetBuilder,
                                                             Preprocessor<K, V> preprocessor) {
        if (!learnLabels(datasetBuilder, preprocessor))
            return getLastTrainedModelOrThrowEmptyDatasetException(mdl);

        IgniteBiTuple<Double, Long> initAndSampleSize = computeInitialValue(envBuilder, datasetBuilder, preprocessor);
        if (initAndSampleSize == null)
            return getLastTrainedModelOrThrowEmptyDatasetException(mdl);

        Double mean = initAndSampleSize.get1();
        Long sampleSize = initAndSampleSize.get2();

        long learningStartTs = System.currentTimeMillis();

        GDBLearningStrategy stgy = getLearningStrategy()
            .withBaseModelTrainerBuilder(this::buildBaseModelTrainer)
            .withExternalLabelToInternal(this::externalLabelToInternal)
            .withCntOfIterations(cntOfIterations)
            .withEnvironmentBuilder(envBuilder)
            .withLossGradient(loss)
            .withSampleSize(sampleSize)
            .withMeanLabelValue(mean)
            .withDefaultGradStepSize(gradientStep)
            .withCheckConvergenceStgyFactory(checkConvergenceStgyFactory);

        List<IgniteModel<Vector, Double>> models;
        if (mdl != null)
            models = stgy.update(mdl, datasetBuilder, preprocessor);
        else
            models = stgy.learnModels(datasetBuilder, preprocessor);

        double learningTime = (double)(System.currentTimeMillis() - learningStartTs) / 1000.0;
        environment.logger(getClass()).log(MLLogger.VerboseLevel.LOW, "The training time was %.2fs", learningTime);

        WeightedPredictionsAggregator resAggregator = new WeightedPredictionsAggregator(
            stgy.getCompositionWeights(),
            stgy.getMeanValue()
        );
        return new GDBModel(models, resAggregator, this::internalLabelToExternal);
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(GDBModel mdl) {
        return mdl instanceof GDBModel;
    }

    /** {@inheritDoc} */
    @Override public GDBTrainer withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (GDBTrainer)super.withEnvironmentBuilder(envBuilder);
    }

    /**
     * Defines unique labels in dataset if need (useful in case of classification).
     *
     * @param builder Dataset builder.
     * @param preprocessor Upstream preprocessor.
     * @return True if labels learning was successful.
     */
    protected abstract <V, K> boolean learnLabels(DatasetBuilder<K, V> builder,
                                                  Preprocessor<K, V> preprocessor);

    /**
     * Returns regressor model trainer for one step of GDB.
     */
    @NotNull
    protected abstract DatasetTrainer<? extends IgniteModel<Vector, Double>, Double> buildBaseModelTrainer();

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
     * @param envBuilder Learning environment builder.
     * @param preprocessor Vectorizer.
     */
    protected <V, K, C extends Serializable> IgniteBiTuple<Double, Long> computeInitialValue(
        LearningEnvironmentBuilder envBuilder,
        DatasetBuilder<K, V> builder,
        Preprocessor<K, V> preprocessor) {
        learningEnvironment().initDeployingContext(preprocessor);

        try (Dataset<EmptyContext, DecisionTreeData> dataset = builder.build(
            envBuilder,
            new EmptyContextBuilder<>(),
            new DecisionTreeDataBuilder<>(preprocessor, false),
            learningEnvironment()
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

            if (meanTuple != null)
                meanTuple.set1(meanTuple.get1() / meanTuple.get2());
            return meanTuple;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets CheckConvergenceStgyFactory.
     *
     * @param factory Factory.
     * @return Trainer.
     */
    public GDBTrainer withCheckConvergenceStgyFactory(ConvergenceCheckerFactory factory) {
        this.checkConvergenceStgyFactory = factory;
        return this;
    }

    /**
     * Returns learning strategy.
     *
     * @return Learning strategy.
     */
    protected GDBLearningStrategy getLearningStrategy() {
        return new GDBLearningStrategy();
    }
}
