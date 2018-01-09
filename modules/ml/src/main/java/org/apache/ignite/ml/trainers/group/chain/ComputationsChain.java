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
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
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
 * // TODO: IGNITE-7322 check if it is possible to integrate with {@link EntryProcessor}.
 */
@FunctionalInterface
public interface ComputationsChain<L extends HasTrainingUUID, K, V, I, O> {
    /**
     * Process given input and {@link GroupTrainingContext}.
     *
     * @param input Computation chain input.
     * @param ctx {@link GroupTrainingContext}.
     * @return Result of processing input and context.
     */
    O process(I input, GroupTrainingContext<K, V, L> ctx);

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
     * Add a distributed step which works in the following way:
     * 1. apply local context and input to local context extractor and keys supplier to get corresponding suppliers;
     * 2. on each node_n
     * 2.1. get context object.
     * 2.2. for each entry_i e located on node_n with key_i from keys stream compute worker((context, entry_i)) and get
     * (cachesUpdates_i, result_i).
     * 2.3. for all i on node_n merge cacheUpdates_i and apply them.
     * 2.4. for all i on node_n, reduce result_i into result_n.
     * 3. get all result_n, reduce them into result and return result.
     *
     * @param <O1> Type of worker output.
     * @param <G> Type of context used by worker.
     * @param workerCtxExtractor Extractor of context for worker.
     * @param worker Function computed on each entry of cache used for training. Second argument is context:
     * common part of data which is independent from key.
     * @param ks Function from chain input and local context to supplier of keys for worker.
     * @param reducer Function used for reducing results of worker.
     * @param identity Identity for reducer.
     * @return Combination of this chain and distributed step specified by given parameters.
     */
    default <O1 extends Serializable, G> ComputationsChain<L, K, V, I, O1> thenDistributedForEntries(
        IgniteBiFunction<O, L, IgniteSupplier<G>> workerCtxExtractor,
        IgniteFunction<EntryAndContext<K, V, G>, ResultAndUpdates<O1>> worker,
        IgniteBiFunction<O, L, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> ks,
        IgniteBinaryOperator<O1> reducer, O1 identity) {
        ComputationsChain<L, K, V, O, O1> nextStep = (input, context) -> {
            L locCtx = context.localContext();
            IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier = ks.apply(input, locCtx);

            Ignite ignite = context.ignite();
            UUID trainingUUID = context.localContext().trainingUUID();
            String cacheName = context.cache().getName();
            ClusterGroup grp = ignite.cluster().forDataNodes(cacheName);

            // Apply first two arguments locally because it is common for all nodes.
            IgniteSupplier<G> extractor = Functions.curry(workerCtxExtractor).apply(input).apply(locCtx);

            return ignite.compute(grp).execute(new GroupTrainerEntriesProcessorTask<>(trainingUUID, extractor, worker, keysSupplier, reducer, identity, cacheName, ignite), null);
        };
        return then(nextStep);
    }

    /**
     * Add a distributed step which works in the following way:
     * 1. apply local context and input to local context extractor and keys supplier to get corresponding suppliers;
     * 2. on each node_n
     * 2.1. get context object.
     * 2.2. for each key_i from keys stream such that key_i located on node_n compute worker((context, entry_i)) and get
     * (cachesUpdates_i, result_i).
     * 2.3. for all i on node_n merge cacheUpdates_i and apply them.
     * 2.4. for all i on node_n, reduce result_i into result_n.
     * 3. get all result_n, reduce them into result and return result.
     *
     * @param <O1> Type of worker output.
     * @param <G> Type of context used by worker.
     * @param workerCtxExtractor Extractor of context for worker.
     * @param worker Function computed on each entry of cache used for training. Second argument is context:
     * common part of data which is independent from key.
     * @param keysSupplier Function from chain input and local context to supplier of keys for worker.
     * @param reducer Function used for reducing results of worker.
     * @param identity Identity for reducer.
     * @return Combination of this chain and distributed step specified by given parameters.
     */
    default <O1 extends Serializable, G> ComputationsChain<L, K, V, I, O1> thenDistributedForKeys(
        IgniteBiFunction<O, L, IgniteSupplier<G>> workerCtxExtractor,
        IgniteFunction<KeyAndContext<K, G>, ResultAndUpdates<O1>> worker,
        IgniteBiFunction<O, L, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> keysSupplier,
        IgniteBinaryOperator<O1> reducer, O1 identity) {
        ComputationsChain<L, K, V, O, O1> nextStep = (input, context) -> {
            L locCtx = context.localContext();
            IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> ks = keysSupplier.apply(input, locCtx);

            Ignite ignite = context.ignite();
            UUID trainingUUID = context.localContext().trainingUUID();
            String cacheName = context.cache().getName();
            ClusterGroup grp = ignite.cluster().forDataNodes(cacheName);

            // Apply first argument locally because it is common for all nodes.
            IgniteSupplier<G> extractor = Functions.curry(workerCtxExtractor).apply(input).apply(locCtx);

            return ignite.compute(grp).execute(new GroupTrainerKeysProcessorTask<>(trainingUUID, extractor, worker, ks, reducer, identity, cacheName, ignite), null);
        };
        return then(nextStep);
    }

    /**
     * Add a distributed step specified by {@link DistributedEntryProcessingStep}.
     *
     * @param step Distributed step.
     * @param <O1> Type of output of distributed step.
     * @param <G> Type of context of distributed step.
     * @return Combination of this chain and distributed step specified by input.
     */
    default <O1 extends Serializable, G> ComputationsChain<L, K, V, I, O1> thenDistributedForEntries(
        DistributedEntryProcessingStep<L, K, V, G, O, O1> step) {
        return thenDistributedForEntries(step::remoteContextSupplier, step.worker(), step::keys, step.reducer(), step.identity());
    }

    /**
     * Add a distributed step specified by {@link DistributedKeyProcessingStep}.
     *
     * @param step Distributed step.
     * @param <O1> Type of output of distributed step.
     * @param <G> Type of context of distributed step.
     * @return Combination of this chain and distributed step specified by input.
     */
    default <O1 extends Serializable, G> ComputationsChain<L, K, V, I, O1> thenDistributedForKeys(
        DistributedKeyProcessingStep<L, K, G, O, O1> step) {
        return thenDistributedForKeys(step::remoteContextSupplier, step.worker(), step::keys, step.reducer(), step.identity());
    }

    /**
     * Version of 'thenDistributedForKeys' where worker does not depend on context.
     *
     * @param worker Worker.
     * @param kf Function providing supplier
     * @param reducer Function from chain input and local context to supplier of keys for worker.
     * @param <O1> Type of worker output.
     * @return Combination of this chain and distributed step specified by given parameters.
     */
    default <O1 extends Serializable> ComputationsChain<L, K, V, I, O1> thenDistributedForKeys(
        IgniteFunction<GroupTrainerCacheKey<K>, ResultAndUpdates<O1>> worker,
        IgniteBiFunction<O, L, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> kf,
        IgniteBinaryOperator<O1> reducer) {

        return thenDistributedForKeys((o, lc) -> () -> o, (context) -> worker.apply(context.key()), kf, reducer, null);
    }

    /**
     * Combine this computation chain with other computation chain in the following way:
     * 1. perform this calculations chain and get result r.
     * 2. while 'cond(r)' is true, r = otherChain(r, context)
     * 3. return r.
     *
     * @param cond Condition checking if 'while' loop should continue.
     * @param otherChain Chain to be combined with this chain.
     * @return Combination of this chain and otherChain.
     */
    default ComputationsChain<L, K, V, I, O> thenWhile(IgniteBiPredicate<O, L> cond,
        ComputationsChain<L, K, V, O, O> otherChain) {
        ComputationsChain<L, K, V, I, O> me = this;
        return (input, context) -> {
            O res = me.process(input, context);

            while (cond.apply(res, context.localContext()))
                res = otherChain.process(res, context);

            return res;
        };
    }

    /**
     * Combine two this chain to other: feed this chain as input to other, pass same context as second argument to both chains
     * process method.
     *
     * @param next Next chain.
     * @param <O1> Type of next chain output.
     * @return Combined chain.
     */
    default <O1> ComputationsChain<L, K, V, I, O1> then(ComputationsChain<L, K, V, O, O1> next) {
        ComputationsChain<L, K, V, I, O> me = this;
        return (input, context) -> {
            O myRes = me.process(input, context);
            return next.process(myRes, context);
        };
    }
}
