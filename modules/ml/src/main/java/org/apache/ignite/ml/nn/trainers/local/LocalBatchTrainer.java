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
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.nn.LocalBatchTrainerInput;
import org.apache.ignite.ml.nn.updaters.ParameterUpdater;
import org.apache.ignite.ml.nn.updaters.UpdaterParams;

/**
 * Batch trainer. This trainer is not distributed on the cluster, but input can theoretically read data from
 * Ignite cache.
 */
public class LocalBatchTrainer<M extends Model<Matrix, Matrix>, P extends UpdaterParams<? super M>> implements Trainer<M, LocalBatchTrainerInput<M>> {
    /**
     * Supplier for updater function.
     */
    private final IgniteSupplier<ParameterUpdater<? super M, P>> updaterSupplier;

    /**
     * Error threshold.
     */
    private final double errorThreshold;

    /**
     * Maximal iterations count.
     */
    private final int maxIterations;

    /**
     * Loss function.
     */
    private final IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /**
     * Logger.
     */
    private IgniteLogger log;

    /**
     * Construct a trainer.
     *
     * @param loss Loss function.
     * @param updaterSupplier Supplier of updater function.
     * @param errorThreshold Error threshold.
     * @param maxIterations Maximal iterations count.
     */
    public LocalBatchTrainer(IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss,
        IgniteSupplier<ParameterUpdater<? super M, P>> updaterSupplier, double errorThreshold, int maxIterations) {
        this.loss = loss;
        this.updaterSupplier = updaterSupplier;
        this.errorThreshold = errorThreshold;
        this.maxIterations = maxIterations;
    }

    /** {@inheritDoc} */
    @Override public M train(LocalBatchTrainerInput<M> data) {
        int i = 0;
        M mdl = data.mdl();
        double err;

        ParameterUpdater<? super M, P> updater = updaterSupplier.get();

        P updaterParams = updater.init(mdl, loss);

        while (i < maxIterations) {
            IgniteBiTuple<Matrix, Matrix> batch = data.getBatch();
            Matrix input = batch.get1();
            Matrix truth = batch.get2();

            updaterParams = updater.updateParams(mdl, updaterParams, i, input, truth);

            // Update mdl with updater parameters.
            mdl = updaterParams.update(mdl);

            Matrix predicted = mdl.apply(input);

            int batchSize = input.columnSize();

            err = MatrixUtil.zipFoldByColumns(predicted, truth, (predCol, truthCol) -> loss.apply(truthCol).apply(predCol)).sum() / batchSize;

            debug("Error: " + err);

            if (err < errorThreshold)
                break;

            i++;
        }

        return mdl;
    }

    /**
     * Construct new trainer with the same parameters as this trainer, but with new loss.
     *
     * @param loss New loss function.
     * @return new trainer with the same parameters as this trainer, but with new loss.
     */
    public LocalBatchTrainer withLoss(IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss) {
        return new LocalBatchTrainer<>(loss, updaterSupplier, errorThreshold, maxIterations);
    }

    /**
     * Construct new trainer with the same parameters as this trainer, but with new updater supplier.
     *
     * @param updaterSupplier New updater supplier.
     * @return new trainer with the same parameters as this trainer, but with new updater supplier.
     */
    public LocalBatchTrainer withUpdater(IgniteSupplier<ParameterUpdater<? super M, P>> updaterSupplier) {
        return new LocalBatchTrainer<>(loss, updaterSupplier, errorThreshold, maxIterations);
    }

    /**
     * Construct new trainer with the same parameters as this trainer, but with new error threshold.
     *
     * @param errorThreshold New error threshold.
     * @return new trainer with the same parameters as this trainer, but with new error threshold.
     */
    public LocalBatchTrainer withErrorThreshold(double errorThreshold) {
        return new LocalBatchTrainer<>(loss, updaterSupplier, errorThreshold, maxIterations);
    }

    /**
     * Construct new trainer with the same parameters as this trainer, but with new maximal iterations count.
     *
     * @param maxIterations New maximal iterations count.
     * @return new trainer with the same parameters as this trainer, but with new maximal iterations count.
     */
    public LocalBatchTrainer withMaxIterations(int maxIterations) {
        return new LocalBatchTrainer<>(loss, updaterSupplier, errorThreshold, maxIterations);
    }

    /**
     * Set logger.
     *
     * @param log Logger.
     * @return This object.
     */
    public LocalBatchTrainer setLogger(IgniteLogger log) {
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
