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

package org.apache.ignite.ml.nn.trainers.local;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteDiffirentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.nn.Losses;
import org.apache.ignite.ml.nn.MLP;
import org.apache.ignite.ml.nn.MLPLocalBatchTrainerInput;
import org.apache.ignite.ml.nn.updaters.MLPParameterUpdater;
import org.apache.ignite.ml.nn.updaters.RPropUpdater;

/**
 * Batch trainer for MLP. This trainer is not distributed on the cluster, but input can be theoretically read data from
 * Ignite cache.
 */
public class MLPLocalBatchTrainer implements Trainer<MLP, MLPLocalBatchTrainerInput> {
    /**
     * Default loss function.
     */
    private static final IgniteFunction<Vector, IgniteDiffirentiableVectorToDoubleFunction> DEFAULT_LOSS = Losses.MSE;

    /**
     * Default updater supplier.
     */
    private static final IgniteSupplier<MLPParameterUpdater> DEFAULT_UPDATER_SUPPLIER = RPropUpdater::new;

    /**
     * Default error threshold.
     */
    private static final double DEFAULT_ERROR_THRESHOLD = 1E-5;

    /**
     * Default learning rate.
     */
    private static final double DEFAULT_LEARNING_RATE = 0.1;

    /**
     * Default maximal iterations count.
     */
    private static final int DEFAULT_MAX_ITERATIONS = 100;

    /**
     * Supplier for updater function.
     */
    private final IgniteSupplier<MLPParameterUpdater> updaterSupplier;

    /**
     * Error threshold.
     */
    private final double errorThreshold;

    /**
     * Learning rate.
     */
    private final double learningRate;

    /**
     * Maximal iterations count.
     */
    private final int maxIterations;

    /**
     * Loss function.
     */
    private final IgniteFunction<Vector, IgniteDiffirentiableVectorToDoubleFunction> loss;

    /**
     * Logger.
     */
    private IgniteLogger log;

    /**
     * Construct a trainer.
     *
     * @param loss Loss function.
     * @param updaterSupplier Supplier of updater function.
     * @param learningRate Learning rate.
     * @param errorThreshold Error threshold.
     * @param maxIterations Maximal iterations count.
     */
    public MLPLocalBatchTrainer(IgniteFunction<Vector, IgniteDiffirentiableVectorToDoubleFunction> loss,
        IgniteSupplier<MLPParameterUpdater> updaterSupplier, double learningRate, double errorThreshold, int maxIterations) {
        this.loss = loss;
        this.updaterSupplier = updaterSupplier;
        this.learningRate = learningRate;
        this.errorThreshold = errorThreshold;
        this.maxIterations = maxIterations;
    }

    /**
     * Constructor with default parameters.
     */
    public MLPLocalBatchTrainer() {
        this(DEFAULT_LOSS, DEFAULT_UPDATER_SUPPLIER, DEFAULT_LEARNING_RATE, DEFAULT_ERROR_THRESHOLD, DEFAULT_MAX_ITERATIONS);
    }

    /** {@inheritDoc} */
    @Override public MLP train(MLPLocalBatchTrainerInput data) {
        int i = 0;
        MLP mlp = data.mlp();
        double err;

        MLPLocalBatchTrainerState state = new MLPLocalBatchTrainerState();
        MLPParameterUpdater updater = updaterSupplier.get();

        updater.init(mlp, learningRate, loss);

        while (i < maxIterations) {
            state.setCurrentIteration(i);

            IgniteBiTuple<Matrix, Matrix> batch = data.getBatch();
            Matrix input = batch.get1();
            Matrix truth = batch.get2();

            err = updater.updateParamsAndCalculateError(mlp, state, input, truth);
            debug("Error: " + err);

            state.setCurrentError(err);

            if (err < errorThreshold)
                break;

            i++;
        }

        return mlp;
    }

    /**
     * Construct new trainer with the same parameters as this trainer, but with new loss.
     *
     * @param loss New loss function.
     * @return new trainer with the same parameters as this trainer, but with new loss.
     */
    public MLPLocalBatchTrainer withLoss(IgniteFunction<Vector, IgniteDiffirentiableVectorToDoubleFunction> loss) {
        return new MLPLocalBatchTrainer(loss, updaterSupplier, learningRate, errorThreshold, maxIterations);
    }

    /**
     * Construct new trainer with the same parameters as this trainer, but with new updater supplier.
     *
     * @param updaterSupplier New updater supplier.
     * @return new trainer with the same parameters as this trainer, but with new updater supplier.
     */
    public MLPLocalBatchTrainer withUpdater(IgniteSupplier<MLPParameterUpdater> updaterSupplier) {
        return new MLPLocalBatchTrainer(loss, updaterSupplier, learningRate, errorThreshold, maxIterations);
    }

    /**
     * Construct new trainer with the same parameters as this trainer, but with new learning rate.
     *
     * @param learningRate New learning rate.
     * @return new trainer with the same parameters as this trainer, but with new learning rate.
     */
    public MLPLocalBatchTrainer withLearningRate(double learningRate) {
        return new MLPLocalBatchTrainer(loss, updaterSupplier, learningRate, errorThreshold, maxIterations);
    }

    /**
     * Construct new trainer with the same parameters as this trainer, but with new error threshold.
     *
     * @param errorThreshold New error threshold.
     * @return new trainer with the same parameters as this trainer, but with new error threshold.
     */
    public MLPLocalBatchTrainer withErrorThreshold(double errorThreshold) {
        return new MLPLocalBatchTrainer(loss, updaterSupplier, learningRate, errorThreshold, maxIterations);
    }

    /**
     * Construct new trainer with the same parameters as this trainer, but with new maximal iterations count.
     *
     * @param maxIterations New maximal iterations count.
     * @return new trainer with the same parameters as this trainer, but with new maximal iterations count.
     */
    public MLPLocalBatchTrainer withMaxIterations(int maxIterations) {
        return new MLPLocalBatchTrainer(loss, updaterSupplier, learningRate, errorThreshold, maxIterations);
    }

    /**
     * Set logger.
     *
     * @param log Logger.
     * @return This object.
     */
    public MLPLocalBatchTrainer setLogger(IgniteLogger log) {
        this.log = log;

        return this;
    }

    /**
     * Output debug message.
     *
     * @param msg Message.
     */
    private void debug(String msg) {
        if (log != null && log.isDebugEnabled())
            log.debug(msg);
    }
}
