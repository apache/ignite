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
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.updaters.ParameterUpdateCalculator;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;
import org.apache.ignite.ml.trainers.group.MetaoptimizerGroupTrainer;
import org.apache.ignite.ml.trainers.group.ResultAndUpdates;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.util.Utils;

public class MLPGroupUpdateTrainer<P extends Serializable> extends
    MetaoptimizerGroupTrainer<MLPGroupUpdateTrainerLocalContext,
        Void,
        MLPGroupTrainingCacheValue,
        P,
        MultilayerPerceptron,
        P,
        MultilayerPerceptron,
        MLPGroupUpdateTrainerInput<P>,
        MLPGroupUpdateTrainingContext<P>,
        P,
        MLPGroupUpdateTrainingLoopData<P>,
        P> {

    private final IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;
    double tolerance;

    public MLPGroupUpdateTrainer(
        IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss,
        MLPMetaoptimizer<P> metaoptimizer,
        IgniteCache<GroupTrainerCacheKey<Void>, MLPGroupTrainingCacheValue> cache,
        Ignite ignite, double tolerance) {
        super(metaoptimizer, cache, ignite);

        this.loss = loss;
        this.tolerance = tolerance;
    }

    /** {@inheritDoc} */
    @Override protected void initDistributedContext(MLPGroupUpdateTrainerInput<P> data, UUID trainingUUID) {
        MLPGroupUpdateTrainerContextCache.getOrCreate(ignite).put(trainingUUID, new MLPGroupUpdateTrainingData<>(
                data.updateCalculator(),
                data.syncRate(),
                data.oneTrainingUpdatesReducer(),
                data.batchSupplier(),
                loss, // TODO: Check how it is serialized.
                tolerance
            ));
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<GroupTrainerCacheKey<Void>, ResultAndUpdates<P>> distributedInitializer(
        MLPGroupUpdateTrainerInput<P> data) {
        MultilayerPerceptron initPerceptron = data.mdl();
        ParameterUpdateCalculator<MultilayerPerceptron, P> calculator = data.updateCalculator();

        // For each key put initial network into the cache.
        return key -> {
            Ignite ignite = Ignition.localIgnite();

            P initUpdate = calculator.init(initPerceptron, loss);// TODO: Check how it is serialized.

            return ResultAndUpdates.of(initUpdate).updateCache(MLPCache.getOrCreate(ignite), key, new MLPGroupTrainingCacheValue(initPerceptron));
        };
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<EntryAndContext<Void, MLPGroupTrainingCacheValue, MLPGroupUpdateTrainingContext<P>>, MLPGroupUpdateTrainingLoopData<P>> trainingLoopStepDataExtractor() {
        return entryAndContext -> {
            MLPGroupUpdateTrainingContext<P> ctx = entryAndContext.context();
            Map.Entry<GroupTrainerCacheKey<Void>, MLPGroupTrainingCacheValue> entry = entryAndContext.entry();
            MLPGroupUpdateTrainingData<P> data = ctx.data();

            return new MLPGroupUpdateTrainingLoopData<>(entry.getValue().perceptron(),
                data.updateCalculator(), data.stepsCnt(), data.updateReducer(), ctx.previousUpdate(), entry.getKey(), data.batchSupplier(), data.loss(), data.tolerance());
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
    @Override protected IgniteSupplier<MLPGroupUpdateTrainingContext<P>> remoteContextExtractor(P prevUpdate,
        MLPGroupUpdateTrainerLocalContext ctx) {
        UUID uuid = ctx.trainingUUID();

        return () -> {
            MLPGroupUpdateTrainingData<P> data = MLPGroupUpdateTrainerContextCache.getOrCreate(Ignition.localIgnite()).get(uuid);
            return new MLPGroupUpdateTrainingContext<>(data, prevUpdate);
        };
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<MLPGroupUpdateTrainingLoopData<P>, ResultAndUpdates<P>> dataProcessor() {
        return data -> {
            MultilayerPerceptron mlp = data.mlp();

            MultilayerPerceptron mlpCp = Utils.copy(mlp);
            ParameterUpdateCalculator<MultilayerPerceptron, P> updateCalculator = data.updateCalculator();
            IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss = data.loss();

            // TODO: This is done just to set loss, and ignore initial update, maybe we should change ParameterUpdateCalculator API to
            // have proper way to setting loss.
            updateCalculator.init(mlpCp, loss);

            P curUpdate = data.previousUpdate();

            int steps = data.stepsCnt();
            List<P> updates = new ArrayList<>(steps);

            IgniteBiTuple<Matrix, Matrix> batch = data.batchSupplier().get();

            for (int i = 0; i < steps; i++) {
                Matrix input = batch.get1();
                Matrix truth = batch.get2();

                int batchSize = truth.columnSize();

                Matrix predicted = mlpCp.apply(truth);

                double err = MatrixUtil.zipFoldByColumns(predicted, truth, (predCol, truthCol) ->
                    loss.apply(truthCol).apply(predCol)).sum() / batchSize;

                if (err < data.tolerance())
                    break;

                updates.add(curUpdate);

                P update = updateCalculator.calculateNewUpdate(mlpCp, curUpdate, i, input, truth);
                mlpCp = updateCalculator.update(mlpCp, update);
            }

            P update = data.getUpdateReducer().apply(updates);

            MultilayerPerceptron newMlp = updateCalculator.update(mlp, data.previousUpdate());

            return new ResultAndUpdates<>(update).
                updateCache(MLPCache.getOrCreate(Ignition.localIgnite()), data.key(), new MLPGroupTrainingCacheValue(newMlp));
        };
    }

    /** {@inheritDoc} */
    @Override protected MLPGroupUpdateTrainerLocalContext<P> initialLocalContext(MLPGroupUpdateTrainerInput<P> data,
        UUID trainingUUID) {
        return new MLPGroupUpdateTrainerLocalContext<>(trainingUUID, data.globalSteps(), data.allUpdatesReducer(), data.trainingsCount());
    }

    @Override protected IgniteSupplier<Stream<GroupTrainerCacheKey<Void>>> finalResultKeys(P data,
        MLPGroupUpdateTrainerLocalContext locCtx) {
        UUID uuid = locCtx.trainingUUID();
        int trainingsCnt = locCtx.parallelTrainingsCnt();

        return () -> MLPCache.allKeys(trainingsCnt, uuid);
    }

    /** {@inheritDoc} */
    @Override protected IgniteSupplier<MLPGroupUpdateTrainingContext<P>> extractContextForFinalResultCreation(P data,
        MLPGroupUpdateTrainerLocalContext locCtx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<EntryAndContext<Void, MLPGroupTrainingCacheValue, MLPGroupUpdateTrainingContext<P>>, ResultAndUpdates<MultilayerPerceptron>> finalResultsExtractor() {
        return context -> ResultAndUpdates.of(context.entry().getValue().perceptron());
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<List<MultilayerPerceptron>, MultilayerPerceptron> finalResultsReducer() {
        // Just take any of MLPs since they will be in the same state.
        return mlps -> !mlps.isEmpty() ? mlps.get(0) : null;
    }

    /** {@inheritDoc} */
    @Override protected MultilayerPerceptron mapFinalResult(MultilayerPerceptron res, MLPGroupUpdateTrainerLocalContext locCtx) {
        return res;
    }

    /** {@inheritDoc} */
    @Override protected void cleanup(MLPGroupUpdateTrainerLocalContext locCtx) {

    }
}
