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

package org.apache.ignite.ml.trainers.group.chain;

import java.io.Serializable;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;
import org.apache.ignite.ml.trainers.group.GroupTrainerEntriesProcessorTask;
import org.apache.ignite.ml.trainers.group.GroupTrainerKeysProcessorTask;
import org.apache.ignite.ml.trainers.group.GroupTrainingContext;
import org.apache.ignite.ml.trainers.group.ResultAndUpdates;

/**
 * This class encapsulates convenient way for creating computations chain for distributed model training.
 * Chain is meant in the sense that output of each non-final computation is fed as input to next computation.
 * Chain is basically a bi-function from context and input to output, context is separated from input
 * because input is specific to each individual step and context is something which is convenient to have access to in each of steps.
 * Context is separated into two parts: local context and remote context.
 * There are two kinds of computations: local and distributed.
 * Local steps are just functions from two arguments: input and local context.
 * Distributed steps are more sophisticated, but basically can be thought as functions of form
 * localContext -> (function of remote context -> output), locally we fix local context and get function
 * (function of remote context -> output) which is executed distributed.
 * Chains are composable through 'then' method.
 *
 * @param <L> Type of local context.
 * @param <K> Type of cache keys.
 * @param <V> Type of cache values.
 * @param <I> Type of input of this chain.
 * @param <O> Type of output of this chain.
 */
public interface ComputationsChain<L extends HasTrainingUUID, K, V, I, O> extends BaseComputationsChain<I, GroupTrainingContext<K, V, L>, O> {
    default ComputationsChain<L, K, V, I, I> create() {
        return (input, context) -> input;
    }

    /**
     * Add a local step to this chain.
     *
     * @param locStep Local step.
     * @param <O1> Output of local step.
     * @return Composition of this chain and local step.
     */
    default <O1> ComputationsChain<L, K, V, I, O1> thenLocally(IgniteBiFunction<O, L, O1> locStep) {
        ComputationsChain<L, K, V, O, O1> nextStep = (input, context) -> locStep.apply(input, context.localContext());
        return then(nextStep);
    }

    /**
     * Add a distributed step
     *
     * @param remoteCtxExtractor
     * @param worker
     * @param kf
     * @param identity
     * @param reducer
     * @param <O1>
     * @param <G>
     * @return
     */
    default <O1 extends Serializable, G> ComputationsChain<L, K, V, I, O1> thenDistributedForEntries(
        IgniteBiFunction<O, L, G> remoteCtxExtractor,
        IgniteFunction<EntryAndContext<K, V, G>, ResultAndUpdates<O1>> worker,
        IgniteBiFunction<O, L, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> kf,
        O1 identity,
        IgniteBinaryOperator<O1> reducer) {
        ComputationsChain<L, K, V, O, O1> nextStep = (input, context) -> {
            L locCtx = context.localContext();
            IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier = kf.apply(input, locCtx);

            Ignite ignite = context.ignite();
            UUID trainingUUID = context.localContext().trainingUUID();
            String cacheName = context.cacheContext().cacheName();
            ClusterGroup grp = ignite.cluster().forDataNodes(cacheName);

            // Apply first argument locally because it is common for all nodes.
            IgniteSupplier<G> extractor = Functions.outputSupplier(Functions.curry(remoteCtxExtractor).apply(input)).apply(locCtx);

            return ignite.compute(grp).execute(new GroupTrainerEntriesProcessorTask<>(trainingUUID, extractor, worker, keysSupplier, reducer, identity, cacheName, ignite), null);
        };
        return then(nextStep);
    }

    default <O1 extends Serializable, G> ComputationsChain<L, K, V, I, O1> thenDistributedForKeys(
        IgniteBiFunction<O, L, G> remoteCtxExtractor,
        IgniteFunction<KeyAndContext<K, G>, ResultAndUpdates<O1>> distributedWorker,
        IgniteBiFunction<O, L, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> kf,
        O1 identity,
        IgniteBinaryOperator<O1> reducer) {
        ComputationsChain<L, K, V, O, O1> nextStep = (input, context) -> {
            L locCtx = context.localContext();
            IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier = kf.apply(input, locCtx);

            Ignite ignite = context.ignite();
            UUID trainingUUID = context.localContext().trainingUUID();
            String cacheName = context.cacheContext().cacheName();
            ClusterGroup grp = ignite.cluster().forDataNodes(cacheName);

            // Apply first argument locally because it is common for all nodes.
            IgniteSupplier<G> extractor = Functions.outputSupplier(Functions.curry(remoteCtxExtractor).apply(input)).apply(locCtx);

            return ignite.compute(grp).execute(new GroupTrainerKeysProcessorTask<>(trainingUUID, extractor, distributedWorker, keysSupplier, reducer, identity, cacheName, ignite), null);
        };
        return then(nextStep);
    }

    default <O1 extends Serializable, G> ComputationsChain<L, K, V, I, O1> thenDistributedForEntries(DistributedStep<L, K, V, G, O, O1> step) {
        return thenDistributedForEntries(step::extractRemoteContext, step::worker, step::keysSupplier, step.identity(), step::reduce);
    }

    default <O1 extends Serializable> ComputationsChain<L, K, V, I, O1> thenDistributedForKeys(
        IgniteFunction<GroupTrainerCacheKey<K>, ResultAndUpdates<O1>> distributedWorker,
        IgniteBiFunction<O, L, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> kf,
        O1 identity,
        IgniteBinaryOperator<O1> reducer) {

        return thenDistributedForKeys((o, lc) -> null, (context) -> distributedWorker.apply(context.key()), kf, identity, reducer);
    }

    default <O1 extends Serializable> ComputationsChain<L, K, V, I, O1> thenDistributedForKeys(
        IgniteBiFunction<O, GroupTrainerCacheKey<K>, ResultAndUpdates<O1>> distributedWorker,
        IgniteBiFunction<O, L, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> kf,
        IgniteBinaryOperator<O1> reducer) {

        return thenDistributedForKeys((o, lc) -> o, (context) -> distributedWorker.apply(context.context(), context.key()), kf, null, reducer);
    }

    default ComputationsChain<L, K, V, I, O> thenWhile(IgniteBiPredicate<O, L> cond, ComputationsChain<L, K, V, O, O> chain) {
        ComputationsChain<L, K, V, I, O> me = this;
        return (input, context) -> {
            O res = me.process(input, context);

            while (cond.apply(res, context.localContext()))
                res = chain.process(res, context);

            return res;
        };
    }

    default <C1, O1> ComputationsChain<L, K, V, I, O1> withOtherContext(IComputationsChain<O, C1, O1> newChain, C1 otherContext) {
        return (input, context) -> {
            O res = process(input, context);
            return newChain.process(res, otherContext);
        };
    }

    default <O1> ComputationsChain<L, K, V, I, O1> then(ComputationsChain<L, K, V, O, O1> next) {
        ComputationsChain<L, K, V, I, O> me = this;
        return (input, context) -> {
            O myRes = me.process(input, context);
            return next.process(myRes, context);
        };
    }
}
