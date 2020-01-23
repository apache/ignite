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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceChecker;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckerFactory;
import org.apache.ignite.ml.composition.boosting.convergence.mean.MeanAbsValueConvergenceCheckerFactory;
import org.apache.ignite.ml.composition.boosting.loss.Loss;
import org.apache.ignite.ml.composition.predictionsaggregator.WeightedPredictionsAggregator;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.jetbrains.annotations.NotNull;

/**
 * Learning strategy for gradient boosting.
 */
public class GDBLearningStrategy {
    /** Learning environment builder. */
    protected LearningEnvironmentBuilder envBuilder;

    /** Learning environment used for trainer. */
    protected LearningEnvironment trainerEnvironment;

    /** Count of iterations. */
    protected int cntOfIterations;

    /** Loss of gradient. */
    protected Loss loss;

    /** External label to internal mapping. */
    protected IgniteFunction<Double, Double> externalLbToInternalMapping;

    /** Base model trainer builder. */
    protected IgniteSupplier<DatasetTrainer<? extends IgniteModel<Vector, Double>, Double>> baseMdlTrainerBuilder;

    /** Mean label value. */
    protected double meanLbVal;

    /** Sample size. */
    protected long sampleSize;

    /** Composition weights. */
    protected double[] compositionWeights;

    /** Check convergence strategy factory. */
    protected ConvergenceCheckerFactory checkConvergenceStgyFactory = new MeanAbsValueConvergenceCheckerFactory(0.001);

    /** Default gradient step size. */
    private double defaultGradStepSize;

    /**
     * Implementation of gradient boosting iterations. At each step of iterations this algorithm build a regression
     * model based on gradient of loss-function for current models composition.
     *
     * @param datasetBuilder Dataset builder.
     * @param preprocessor Upstream preprocessor.
     * @return List of learned models.
     */
    public <K, V> List<IgniteModel<Vector, Double>> learnModels(DatasetBuilder<K, V> datasetBuilder,
                                                                Preprocessor<K, V> preprocessor) {

        return update(null, datasetBuilder, preprocessor);
    }

    /**
     * Gets state of model in arguments, compare it with training parameters of trainer and if they are fit then trainer
     * updates model in according to new data and return new model. In other case trains new model.
     *
     * @param mdlToUpdate Learned model.
     * @param datasetBuilder Dataset builder.
     * @param preprocessor Upstream preprocessor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated models list.
     */
    public <K, V> List<IgniteModel<Vector, Double>> update(GDBTrainer.GDBModel mdlToUpdate,
                                                           DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> preprocessor) {
        if (trainerEnvironment == null)
            throw new IllegalStateException("Learning environment builder is not set.");

        List<IgniteModel<Vector, Double>> models = initLearningState(mdlToUpdate);

        ConvergenceChecker<K, V> convCheck = checkConvergenceStgyFactory.create(sampleSize,
            externalLbToInternalMapping, loss, datasetBuilder, preprocessor);

        DatasetTrainer<? extends IgniteModel<Vector, Double>, Double> trainer = baseMdlTrainerBuilder.get();
        for (int i = 0; i < cntOfIterations; i++) {
            double[] weights = Arrays.copyOf(compositionWeights, models.size());

            WeightedPredictionsAggregator aggregator = new WeightedPredictionsAggregator(weights, meanLbVal);
            ModelsComposition currComposition = new ModelsComposition(models, aggregator);
            if (convCheck.isConverged(envBuilder, datasetBuilder, currComposition))
                break;

            Vectorizer<K, V, Serializable, Double> extractor = new Vectorizer.VectorizerAdapter<K, V, Serializable, Double>() {
                /** {@inheritDoc} */
                @Override public LabeledVector<Double> extract(K k, V v) {
                    LabeledVector<Double> labeledVector = preprocessor.apply(k, v);
                    Vector features = labeledVector.features();
                    Double realAnswer = externalLbToInternalMapping.apply(labeledVector.label());
                    Double mdlAnswer = currComposition.predict(features);
                    return new LabeledVector<>(features, -loss.gradient(sampleSize, realAnswer, mdlAnswer));
                }
            };

            long startTs = System.currentTimeMillis();
            models.add(trainer.fit(datasetBuilder, extractor));
            double learningTime = (double)(System.currentTimeMillis() - startTs) / 1000.0;
            trainerEnvironment.logger(getClass()).log(MLLogger.VerboseLevel.LOW, "One model training time was %.2fs", learningTime);
        }

        return models;
    }

    /**
     * Restores state of already learned model if can and sets learning parameters according to this state.
     *
     * @param mdlToUpdate Model to update.
     * @return List of already learned models.
     */
    @NotNull protected List<IgniteModel<Vector, Double>> initLearningState(GDBTrainer.GDBModel mdlToUpdate) {
        List<IgniteModel<Vector, Double>> models = new ArrayList<>();
        if (mdlToUpdate != null) {
            models.addAll(mdlToUpdate.getModels());
            WeightedPredictionsAggregator aggregator = (WeightedPredictionsAggregator)mdlToUpdate.getPredictionsAggregator();
            meanLbVal = aggregator.getBias();
            compositionWeights = new double[models.size() + cntOfIterations];
            System.arraycopy(aggregator.getWeights(), 0, compositionWeights, 0, models.size());
        }
        else
            compositionWeights = new double[cntOfIterations];

        Arrays.fill(compositionWeights, models.size(), compositionWeights.length, defaultGradStepSize);
        return models;
    }

    /**
     * Sets learning environment.
     *
     * @param envBuilder Learning Environment.
     */
    public GDBLearningStrategy withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        this.envBuilder = envBuilder;
        this.trainerEnvironment = envBuilder.buildForTrainer();
        return this;
    }

    /**
     * Sets count of iterations.
     *
     * @param cntOfIterations Count of iterations.
     */
    public GDBLearningStrategy withCntOfIterations(int cntOfIterations) {
        this.cntOfIterations = cntOfIterations;
        return this;
    }

    /**
     * Loss function.
     *
     * @param loss Loss function.
     */
    public GDBLearningStrategy withLossGradient(Loss loss) {
        this.loss = loss;
        return this;
    }

    /**
     * Sets external to internal label representation mapping.
     *
     * @param externalLbToInternal External label to internal.
     */
    public GDBLearningStrategy withExternalLabelToInternal(IgniteFunction<Double, Double> externalLbToInternal) {
        this.externalLbToInternalMapping = externalLbToInternal;
        return this;
    }

    /**
     * Sets base model builder.
     *
     * @param buildBaseMdlTrainer Build base model trainer.
     */
    public GDBLearningStrategy withBaseModelTrainerBuilder(
        IgniteSupplier<DatasetTrainer<? extends IgniteModel<Vector, Double>, Double>> buildBaseMdlTrainer) {
        this.baseMdlTrainerBuilder = buildBaseMdlTrainer;
        return this;
    }

    /**
     * Sets mean label value.
     *
     * @param meanLbVal Mean label value.
     */
    public GDBLearningStrategy withMeanLabelValue(double meanLbVal) {
        this.meanLbVal = meanLbVal;
        return this;
    }

    /**
     * Sets sample size.
     *
     * @param sampleSize Sample size.
     */
    public GDBLearningStrategy withSampleSize(long sampleSize) {
        this.sampleSize = sampleSize;
        return this;
    }

    /**
     * Sets composition weights vector.
     *
     * @param compositionWeights Composition weights.
     */
    public GDBLearningStrategy withCompositionWeights(double[] compositionWeights) {
        this.compositionWeights = compositionWeights;
        return this;
    }

    /**
     * Sets CheckConvergenceStgyFactory.
     *
     * @param factory Factory.
     */
    public GDBLearningStrategy withCheckConvergenceStgyFactory(ConvergenceCheckerFactory factory) {
        this.checkConvergenceStgyFactory = factory;
        return this;
    }

    /**
     * Sets default gradient step size.
     *
     * @param defaultGradStepSize Default gradient step size.
     */
    public GDBLearningStrategy withDefaultGradStepSize(double defaultGradStepSize) {
        this.defaultGradStepSize = defaultGradStepSize;
        return this;
    }

    /** */
    public double[] getCompositionWeights() {
        return compositionWeights;
    }

    /** */
    public double getMeanValue() {
        return meanLbVal;
    }
}
