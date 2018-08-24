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

package org.apache.ignite.ml.regressions.logistic.binomial;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.dataset.primitive.data.SimpleLabeledDatasetData;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.nn.MLPTrainer;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * Trainer of the logistic regression model based on stochastic gradient descent algorithm.
 */
public class LogisticRegressionSGDTrainer<P extends Serializable> extends SingleLabelDatasetTrainer<LogisticRegressionModel> {
    /** Update strategy. */
    private final UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy;

    /** Max number of iteration. */
    private final int maxIterations;

    /** Batch size. */
    private final int batchSize;

    /** Number of local iterations. */
    private final int locIterations;

    /** Seed for random generator. */
    private final long seed;

    /**
     * Constructs a new instance of linear regression SGD trainer.
     *
     * @param updatesStgy Update strategy.
     * @param maxIterations Max number of iteration.
     * @param batchSize Batch size.
     * @param locIterations Number of local iterations.
     * @param seed Seed for random generator.
     */
    public LogisticRegressionSGDTrainer(UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy, int maxIterations,
                                        int batchSize, int locIterations, long seed) {
        this.updatesStgy = updatesStgy;
        this.maxIterations = maxIterations;
        this.batchSize = batchSize;
        this.locIterations = locIterations;
        this.seed = seed;
    }

    /** {@inheritDoc} */
    @Override public <K, V> LogisticRegressionModel fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        IgniteFunction<Dataset<EmptyContext, SimpleLabeledDatasetData>, MLPArchitecture> archSupplier = dataset -> {

            int cols = dataset.compute(data -> {
                if (data.getFeatures() == null)
                    return null;
                return data.getFeatures().length / data.getRows();
            }, (a, b) -> a == null ? b : a);

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
        );

        MultilayerPerceptron mlp = trainer.fit(datasetBuilder, featureExtractor, (k, v) -> new double[]{lbExtractor.apply(k, v)});

        double[] params = mlp.parameters().getStorage().data();

        return new LogisticRegressionModel(new DenseVector(Arrays.copyOf(params, params.length - 1)),
            params[params.length - 1]
        );
    }
}
