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

package org.apache.ignite.ml.regressions.logistic;

import java.util.Arrays;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.dataset.primitive.data.SimpleLabeledDatasetData;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.nn.MLPTrainer;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.developer.PatchedPreprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;
import org.jetbrains.annotations.NotNull;

/**
 * Trainer of the logistic regression model based on stochastic gradient descent algorithm.
 */
public class LogisticRegressionSGDTrainer extends SingleLabelDatasetTrainer<LogisticRegressionModel> {
    /** Update strategy. */
    private UpdatesStrategy updatesStgy = new UpdatesStrategy<>(
        new SimpleGDUpdateCalculator(0.2),
        SimpleGDParameterUpdate.SUM_LOCAL,
        SimpleGDParameterUpdate.AVG
    );

    /** Max number of iteration. */
    private int maxIterations = 100;

    /** Batch size. */
    private int batchSize = 100;

    /** Number of local iterations. */
    private int locIterations = 100;

    /** Seed for random generator. */
    private long seed = 1234L;

    /** {@inheritDoc} */
    @Override public <K, V> LogisticRegressionModel fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> extractor) {

        return updateModel(null, datasetBuilder, extractor);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> LogisticRegressionModel updateModel(LogisticRegressionModel mdl,
        DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> extractor) {

        IgniteFunction<Dataset<EmptyContext, SimpleLabeledDatasetData>, MLPArchitecture> archSupplier = dataset -> {
            Integer cols = dataset.compute(data -> {
                if (data.getFeatures() == null)
                    return null;
                return data.getFeatures().length / data.getRows();
            }, (a, b) -> {
                // If both are null then zero will be propagated, no good.
                if (a == null)
                    return b;
                return a;
            });

            if (cols == null)
                throw new IllegalStateException("Cannot train on empty dataset");

            MLPArchitecture architecture = new MLPArchitecture(cols);
            architecture = architecture.withAddedLayer(1, true, Activators.SIGMOID);

            return architecture;
        };

        MLPTrainer<?> trainer = new MLPTrainer<>(
            archSupplier,
            LossFunctions.L2,
            updatesStgy,
            maxIterations,
            batchSize,
            locIterations,
            seed
        ).withEnvironmentBuilder(envBuilder);

        MultilayerPerceptron mlp;

        IgniteFunction<LabeledVector<Double>, LabeledVector<double[]>> func = lv -> new LabeledVector<>(lv.features(), new double[] { lv.label()});

        PatchedPreprocessor<K, V, Double, double[]> patchedPreprocessor = new PatchedPreprocessor<>(func, extractor);

        if (mdl != null) {
            mlp = restoreMLPState(mdl);
            mlp = trainer.update(mlp, datasetBuilder, patchedPreprocessor);
        }
        else
            mlp = trainer.fit(datasetBuilder, patchedPreprocessor);

        double[] params = mlp.parameters().getStorage().data();

        return new LogisticRegressionModel(new DenseVector(Arrays.copyOf(params, params.length - 1)),
            params[params.length - 1]
        );
    }

    /**
     * @param mdl Model.
     * @return state of MLP from last learning.
     */
    @NotNull private MultilayerPerceptron restoreMLPState(LogisticRegressionModel mdl) {
        Vector weights = mdl.weights();
        double intercept = mdl.intercept();

        MLPArchitecture architecture1 = new MLPArchitecture(weights.size());
        architecture1 = architecture1.withAddedLayer(1, true, Activators.SIGMOID);

        MLPArchitecture architecture = architecture1;
        MultilayerPerceptron perceptron = new MultilayerPerceptron(architecture);

        Vector mlpState = weights.like(weights.size() + 1);
        weights.nonZeroes().forEach(ith -> mlpState.set(ith.index(), ith.get()));
        mlpState.set(mlpState.size() - 1, intercept);
        perceptron.setParameters(mlpState);

        return perceptron;
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(LogisticRegressionModel mdl) {
        return true;
    }

    /**
     * Set up the max amount of iterations before convergence.
     *
     * @param maxIterations The parameter value.
     * @return Model with new max number of iterations before convergence parameter value.
     */
    public LogisticRegressionSGDTrainer withMaxIterations(double maxIterations) {
        this.maxIterations = (int) maxIterations;
        return this;
    }

    /**
     * Set up the batchSize parameter.
     *
     * @param batchSize The size of learning batch.
     * @return Trainer with new batch size parameter value.
     */
    public LogisticRegressionSGDTrainer withBatchSize(double batchSize) {
        this.batchSize = (int) batchSize;
        return this;
    }

    /**
     * Set up the amount of local iterations of SGD algorithm.
     *
     * @param amountOfLocIterations The parameter value.
     * @return Trainer with new locIterations parameter value.
     */
    public LogisticRegressionSGDTrainer withLocIterations(double amountOfLocIterations) {
        this.locIterations = (int) amountOfLocIterations;
        return this;
    }

    /**
     * Set up the random seed parameter.
     *
     * @param seed Seed for random generator.
     * @return Trainer with new seed parameter value.
     */
    public LogisticRegressionSGDTrainer withSeed(long seed) {
        this.seed = seed;
        return this;
    }

    /**
     * Set up the regularization parameter.
     *
     * @param updatesStgy Update strategy.
     * @return Trainer with new update strategy parameter value.
     */
    public LogisticRegressionSGDTrainer withUpdatesStgy(UpdatesStrategy updatesStgy) {
        this.updatesStgy = updatesStgy;
        return this;
    }

    /**
     * Get the update strategy.
     *
     * @return The property value.
     */
    public UpdatesStrategy getUpdatesStgy() {
        return updatesStgy;
    }

    /**
     * Get the max amount of iterations.
     *
     * @return The property value.
     */
    public int getMaxIterations() {
        return maxIterations;
    }

    /**
     * Get the batch size.
     *
     * @return The property value.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Get the amount of local iterations.
     *
     * @return The property value.
     */
    public int getLocIterations() {
        return locIterations;
    }

    /**
     * Get the seed for random generator.
     *
     * @return The property value.
     */
    public long getSeed() {
        return seed;
    }
}
