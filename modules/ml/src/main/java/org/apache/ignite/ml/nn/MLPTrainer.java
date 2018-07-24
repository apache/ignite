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

package org.apache.ignite.ml.nn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.data.SimpleLabeledDatasetDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.dataset.primitive.data.SimpleLabeledDatasetData;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.initializers.RandomInitializer;
import org.apache.ignite.ml.optimization.updatecalculators.ParameterUpdateCalculator;
import org.apache.ignite.ml.trainers.MultiLabelDatasetTrainer;
import org.apache.ignite.ml.util.Utils;

/**
 * Multilayer perceptron trainer based on partition based {@link Dataset}.
 *
 * @param <P> Type of model update used in this trainer.
 */
public class MLPTrainer<P extends Serializable> extends MultiLabelDatasetTrainer<MultilayerPerceptron> {
    /** Multilayer perceptron architecture supplier that defines layers and activators. */
    private final IgniteFunction<Dataset<EmptyContext, SimpleLabeledDatasetData>, MLPArchitecture> archSupplier;

    /** Loss function to be minimized during the training. */
    private final IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /** Update strategy that defines how to update model parameters during the training. */
    private final UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy;

    /** Maximal number of iterations before the training will be stopped. */
    private final int maxIterations;

    /** Batch size (per every partition). */
    private final int batchSize;

    /** Maximal number of local iterations before synchronization. */
    private final int locIterations;

    /** Multilayer perceptron model initializer. */
    private final long seed;

    /**
     * Constructs a new instance of multilayer perceptron trainer.
     *
     * @param arch Multilayer perceptron architecture that defines layers and activators.
     * @param loss Loss function to be minimized during the training.
     * @param updatesStgy Update strategy that defines how to update model parameters during the training.
     * @param maxIterations Maximal number of iterations before the training will be stopped.
     * @param batchSize Batch size (per every partition).
     * @param locIterations Maximal number of local iterations before synchronization.
     * @param seed Random initializer seed.
     */
    public MLPTrainer(MLPArchitecture arch, IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss,
        UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy, int maxIterations, int batchSize,
        int locIterations, long seed) {
        this(dataset -> arch, loss, updatesStgy, maxIterations, batchSize, locIterations, seed);
    }

    /**
     * Constructs a new instance of multilayer perceptron trainer.
     *
     * @param archSupplier Multilayer perceptron architecture supplier that defines layers and activators.
     * @param loss Loss function to be minimized during the training.
     * @param updatesStgy Update strategy that defines how to update model parameters during the training.
     * @param maxIterations Maximal number of iterations before the training will be stopped.
     * @param batchSize Batch size (per every partition).
     * @param locIterations Maximal number of local iterations before synchronization.
     * @param seed Random initializer seed.
     */
    public MLPTrainer(IgniteFunction<Dataset<EmptyContext, SimpleLabeledDatasetData>, MLPArchitecture> archSupplier,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss,
        UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy, int maxIterations, int batchSize,
        int locIterations, long seed) {
        this.archSupplier = archSupplier;
        this.loss = loss;
        this.updatesStgy = updatesStgy;
        this.maxIterations = maxIterations;
        this.batchSize = batchSize;
        this.locIterations = locIterations;
        this.seed = seed;
    }

    /** {@inheritDoc} */
    public <K, V> MultilayerPerceptron fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, double[]> lbExtractor) {

        try (Dataset<EmptyContext, SimpleLabeledDatasetData> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(),
            new SimpleLabeledDatasetDataBuilder<>(featureExtractor, lbExtractor)
        )) {
            MLPArchitecture arch = archSupplier.apply(dataset);
            MultilayerPerceptron mdl = new MultilayerPerceptron(arch, new RandomInitializer(seed));
            ParameterUpdateCalculator<? super MultilayerPerceptron, P> updater = updatesStgy.getUpdatesCalculator();

            for (int i = 0; i < maxIterations; i += locIterations) {

                MultilayerPerceptron finalMdl = mdl;
                int finalI = i;

                List<P> totUp = dataset.compute(
                    data -> {
                        P update = updater.init(finalMdl, loss);

                        MultilayerPerceptron mlp = Utils.copy(finalMdl);

                        if (data.getFeatures() != null) {
                            List<P> updates = new ArrayList<>();

                            for (int locStep = 0; locStep < locIterations; locStep++) {
                                int[] rows = Utils.selectKDistinct(
                                    data.getRows(),
                                    Math.min(batchSize, data.getRows()),
                                    new Random(seed ^ (finalI * locStep))
                                );

                                double[] inputsBatch = batch(data.getFeatures(), rows, data.getRows());
                                double[] groundTruthBatch = batch(data.getLabels(), rows, data.getRows());

                                Matrix inputs = new DenseMatrix(inputsBatch, rows.length, 0);
                                Matrix groundTruth = new DenseMatrix(groundTruthBatch, rows.length, 0);

                                update = updater.calculateNewUpdate(
                                    mlp,
                                    update,
                                    locStep,
                                    inputs.transpose(),
                                    groundTruth.transpose()
                                );

                                mlp = updater.update(mlp, update);
                                updates.add(update);
                            }

                            List<P> res = new ArrayList<>();
                            res.add(updatesStgy.locStepUpdatesReducer().apply(updates));

                            return res;
                        }

                        return null;
                    },
                    (a, b) -> {
                        if (a == null)
                            return b;
                        else if (b == null)
                            return a;
                        else {
                            a.addAll(b);
                            return a;
                        }
                    }
                );

                P update = updatesStgy.allUpdatesReducer().apply(totUp);
                mdl = updater.update(mdl, update);
            }

            return mdl;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds a batch of the data by fetching specified rows.
     *
     * @param data All data.
     * @param rows Rows to be fetched from the data.
     * @param totalRows Total number of rows in all data.
     * @return Batch data.
     */
    static double[] batch(double[] data, int[] rows, int totalRows) {
        int cols = data.length / totalRows;

        double[] res = new double[cols * rows.length];

        for (int i = 0; i < rows.length; i++)
            for (int j = 0; j < cols; j++)
                res[j * rows.length + i] = data[j * totalRows + rows[i]];

        return res;
    }
}
