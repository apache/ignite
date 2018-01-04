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

package org.apache.ignite.ml.nn.trainers.distributed;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.nn.LossFunctions;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.updaters.ParameterUpdateCalculator;
import org.apache.ignite.ml.nn.updaters.RPropParameterUpdate;
import org.apache.ignite.ml.nn.updaters.RPropUpdateCalculator;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;
import org.apache.ignite.ml.trainers.group.MetaoptimizerGroupTrainer;
import org.apache.ignite.ml.trainers.group.ResultAndUpdates;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.util.Utils;

/**
 * Update-based distributed training of MLP.
 *
 * @param <U> Type of update.
 */
public class MLPGroupUpdateTrainer<U extends Serializable> extends
    MetaoptimizerGroupTrainer<MLPGroupUpdateTrainerLocalContext,
        Void,
        MLPGroupTrainingCacheValue,
        U,
        MultilayerPerceptron,
        U,
        MultilayerPerceptron,
        AbstractMLPGroupUpdateTrainerInput,
        MLPGroupUpdateTrainingContext<U>,
        ArrayList<U>,
        MLPGroupUpdateTrainingLoopData<U>,
        U> {
    /**
     * Loss function.
     */
    private final IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /**
     * Error tolerance.
     */
    private final double tolerance;

    /**
     * Maximal count of global steps.
     */
    private final int maxGlobalSteps;

    /**
     * Synchronize updates between networks every syncRate steps.
     */
    private final int syncRate;

    /**
     * Function used to reduce updates from different networks (for example, averaging of gradients of all networks).
     */
    private final IgniteFunction<List<U>, U> allUpdatesReducer;

    /**
     * Function used to reduce updates in one training (for example, sum all sequential gradient updates to get one
     * gradient update).
     */
    private final IgniteFunction<List<U>, U> locStepUpdatesReducer;

    /**
     * Updates calculator.
     */
    private final ParameterUpdateCalculator<MultilayerPerceptron, U> updateCalculator;

    /**
     * Default maximal count of global steps.
     */
    private static final int DEFAULT_MAX_GLOBAL_STEPS = 30;

    /**
     * Default sync rate.
     */
    private static final int DEFAULT_SYNC_RATE = 5;

    /**
     * Default all updates reducer.
     */
    private static final IgniteFunction<List<RPropParameterUpdate>, RPropParameterUpdate>
        DEFAULT_ALL_UPDATES_REDUCER = RPropParameterUpdate::avg;

    /**
     * Default local steps updates reducer.
     */
    private static final IgniteFunction<List<RPropParameterUpdate>, RPropParameterUpdate>
        DEFAULT_LOCAL_STEP_UPDATES_REDUCER = RPropParameterUpdate::sumLocal;

    /**
     * Default update calculator.
     */
    private static final ParameterUpdateCalculator<MultilayerPerceptron, RPropParameterUpdate>
        DEFAULT_UPDATE_CALCULATOR = new RPropUpdateCalculator<>();

    /**
     * Default loss function.
     */
    private static final IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> DEFAULT_LOSS
        = LossFunctions.MSE;

    /**
     * Construct instance of this class with given parameters.
     *
     * @param loss Loss function.
     * @param ignite Ignite instance.
     * @param tolerance Error tolerance.
     */
    public MLPGroupUpdateTrainer(int maxGlobalSteps,
        int syncRate,
        IgniteFunction<List<U>, U> allUpdatesReducer,
        IgniteFunction<List<U>, U> locStepUpdatesReducer,
        ParameterUpdateCalculator<MultilayerPerceptron, U> updateCalculator,
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss,
        Ignite ignite, double tolerance) {
        super(new MLPMetaoptimizer<>(allUpdatesReducer), MLPCache.getOrCreate(ignite), ignite);

        this.maxGlobalSteps = maxGlobalSteps;
        this.syncRate = syncRate;
        this.allUpdatesReducer = allUpdatesReducer;
        this.locStepUpdatesReducer = locStepUpdatesReducer;
        this.updateCalculator = updateCalculator;
        this.loss = loss;
        this.tolerance = tolerance;
    }

    /**
     * Get default {@link MLPGroupUpdateTrainer}.
     *
     * @param ignite Ignite instance.
     * @return Default {@link MLPGroupUpdateTrainer}.
     */
    public static MLPGroupUpdateTrainer<RPropParameterUpdate> getDefault(Ignite ignite) {
        return new MLPGroupUpdateTrainer<>(DEFAULT_MAX_GLOBAL_STEPS, DEFAULT_SYNC_RATE, DEFAULT_ALL_UPDATES_REDUCER,
            DEFAULT_LOCAL_STEP_UPDATES_REDUCER, DEFAULT_UPDATE_CALCULATOR, DEFAULT_LOSS, ignite, 0.01);
    }

    /** {@inheritDoc} */
    @Override protected void init(AbstractMLPGroupUpdateTrainerInput data, UUID trainingUUID) {
        super.init(data, trainingUUID);

        MLPGroupUpdateTrainerDataCache.getOrCreate(ignite).put(trainingUUID, new MLPGroupUpdateTrainingData<>(
            updateCalculator,
            syncRate,
            locStepUpdatesReducer,
            data.batchSupplier(),
            loss,
            tolerance
        ));
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<GroupTrainerCacheKey<Void>, ResultAndUpdates<U>> distributedInitializer(
        AbstractMLPGroupUpdateTrainerInput data) {
        MultilayerPerceptron initPerceptron = data.mdl();

        // For each key put initial network into the cache.
        return key -> {
            Ignite ignite = Ignition.localIgnite();

            U initUpdate = updateCalculator.init(initPerceptron, loss);

            return ResultAndUpdates.of(initUpdate).updateCache(MLPCache.getOrCreate(ignite), key,
                new MLPGroupTrainingCacheValue(initPerceptron));
        };
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<EntryAndContext<Void, MLPGroupTrainingCacheValue,
        MLPGroupUpdateTrainingContext<U>>, MLPGroupUpdateTrainingLoopData<U>> trainingLoopStepDataExtractor() {
        return entryAndContext -> {
            MLPGroupUpdateTrainingContext<U> ctx = entryAndContext.context();
            Map.Entry<GroupTrainerCacheKey<Void>, MLPGroupTrainingCacheValue> entry = entryAndContext.entry();
            MLPGroupUpdateTrainingData<U> data = ctx.data();

            return new MLPGroupUpdateTrainingLoopData<>(entry.getValue().perceptron(),
                data.updateCalculator(), data.stepsCnt(), data.updateReducer(), ctx.previousUpdate(), entry.getKey(),
                data.batchSupplier(), data.loss(), data.tolerance());
        };
    }

    /** {@inheritDoc} */
    @Override protected IgniteSupplier<Stream<GroupTrainerCacheKey<Void>>> keysToProcessInTrainingLoop(
        MLPGroupUpdateTrainerLocalContext locCtx) {
        int trainingsCnt = locCtx.parallelTrainingsCnt();
        UUID uuid = locCtx.trainingUUID();

        return () -> MLPCache.allKeys(trainingsCnt, uuid);
    }

    /** {@inheritDoc} */
    @Override protected IgniteSupplier<MLPGroupUpdateTrainingContext<U>> remoteContextExtractor(U prevUpdate,
        MLPGroupUpdateTrainerLocalContext ctx) {
        UUID uuid = ctx.trainingUUID();

        return () -> {
            MLPGroupUpdateTrainingData<U> data = MLPGroupUpdateTrainerDataCache
                .getOrCreate(Ignition.localIgnite()).get(uuid);
            return new MLPGroupUpdateTrainingContext<>(data, prevUpdate);
        };
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<MLPGroupUpdateTrainingLoopData<U>, ResultAndUpdates<U>> dataProcessor() {
        return data -> {
            MultilayerPerceptron mlp = data.mlp();

            MultilayerPerceptron mlpCp = Utils.copy(mlp);
            ParameterUpdateCalculator<MultilayerPerceptron, U> updateCalculator = data.updateCalculator();
            IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss = data.loss();

            // ParameterUpdateCalculator API to have proper way to setting loss.
            updateCalculator.init(mlpCp, loss);

            U curUpdate = data.previousUpdate();

            int steps = data.stepsCnt();
            List<U> updates = new ArrayList<>(steps);

            IgniteBiTuple<Matrix, Matrix> batch = data.batchSupplier().get();

            for (int i = 0; i < steps; i++) {
                Matrix input = batch.get1();
                Matrix truth = batch.get2();

                int batchSize = truth.columnSize();

                Matrix predicted = mlpCp.apply(input);

                double err = MatrixUtil.zipFoldByColumns(predicted, truth, (predCol, truthCol) ->
                    loss.apply(truthCol).apply(predCol)).sum() / batchSize;

                if (err < data.tolerance())
                    break;

                mlpCp = updateCalculator.update(mlpCp, curUpdate);
                updates.add(curUpdate);

                curUpdate = updateCalculator.calculateNewUpdate(mlpCp, curUpdate, i, input, truth);
            }

            U update = data.getUpdateReducer().apply(updates);

            MultilayerPerceptron newMlp = updateCalculator.update(mlp, data.previousUpdate());

            return new ResultAndUpdates<>(update).
                updateCache(MLPCache.getOrCreate(Ignition.localIgnite()), data.key(),
                    new MLPGroupTrainingCacheValue(newMlp));
        };
    }

    /** {@inheritDoc} */
    @Override protected MLPGroupUpdateTrainerLocalContext<U> initialLocalContext(
        AbstractMLPGroupUpdateTrainerInput data, UUID trainingUUID) {
        return new MLPGroupUpdateTrainerLocalContext<>(trainingUUID, maxGlobalSteps, allUpdatesReducer,
            data.trainingsCount());
    }

    /** {@inheritDoc} */
    @Override protected IgniteSupplier<Stream<GroupTrainerCacheKey<Void>>> finalResultKeys(U data,
        MLPGroupUpdateTrainerLocalContext locCtx) {
        UUID uuid = locCtx.trainingUUID();
        int trainingsCnt = locCtx.parallelTrainingsCnt();

        return () -> MLPCache.allKeys(trainingsCnt, uuid);
    }

    /** {@inheritDoc} */
    @Override protected IgniteSupplier<MLPGroupUpdateTrainingContext<U>> extractContextForFinalResultCreation(U data,
        MLPGroupUpdateTrainerLocalContext locCtx) {
        return () -> null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<EntryAndContext<Void, MLPGroupTrainingCacheValue,
        MLPGroupUpdateTrainingContext<U>>, ResultAndUpdates<MultilayerPerceptron>> finalResultsExtractor() {
        return context -> ResultAndUpdates.of(context.entry().getValue().perceptron());
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<List<MultilayerPerceptron>, MultilayerPerceptron> finalResultsReducer() {
        // Just take any of MLPs since they will be in the same state.
        return mlps -> mlps.stream().filter(Objects::nonNull).findFirst().orElse(null);
    }

    /** {@inheritDoc} */
    @Override protected MultilayerPerceptron mapFinalResult(MultilayerPerceptron res,
        MLPGroupUpdateTrainerLocalContext locCtx) {
        return res;
    }

    /** {@inheritDoc} */
    @Override protected void cleanup(MLPGroupUpdateTrainerLocalContext locCtx) {

    }

    /**
     * Create new {@link MLPGroupUpdateTrainer} with new maxGlobalSteps value.
     *
     * @param maxGlobalSteps New maxGlobalSteps value.
     * @return New {@link MLPGroupUpdateTrainer} with new maxGlobalSteps value.
     */
    public MLPGroupUpdateTrainer<U> withMaxGlobalSteps(int maxGlobalSteps) {
        return new MLPGroupUpdateTrainer<>(maxGlobalSteps, syncRate, allUpdatesReducer, locStepUpdatesReducer,
            updateCalculator, loss, ignite, tolerance);
    }

    /**
     * Create new {@link MLPGroupUpdateTrainer} with new syncRate value.
     *
     * @param syncRate New syncRate value.
     * @return New {@link MLPGroupUpdateTrainer} with new syncRate value.
     */
    public MLPGroupUpdateTrainer<U> withSyncRate(int syncRate) {
        return new MLPGroupUpdateTrainer<>(maxGlobalSteps, syncRate
            , allUpdatesReducer, locStepUpdatesReducer, updateCalculator, loss, ignite, tolerance);
    }

    /**
     * Create new {@link MLPGroupUpdateTrainer} with new tolerance.
     *
     * @param tolerance New tolerance value.
     * @return New {@link MLPGroupUpdateTrainer} with new tolerance value.
     */
    public MLPGroupUpdateTrainer<U> withTolerance(double tolerance) {
        return new MLPGroupUpdateTrainer<>(maxGlobalSteps, syncRate, allUpdatesReducer, locStepUpdatesReducer,
            updateCalculator, loss, ignite, tolerance);
    }
}
