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
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteConsumer;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;
import org.apache.ignite.ml.trainers.group.GroupTrainerTask;

public interface DistributedTrainerWorkersChain<L, K, V, I, C extends HasCacheContext<GroupTrainerCacheKey<K>, V> & HasLocalContext<L> & HasTrainingUUID, O> extends BaseWorkersChain<I, C, O> {
    default DistributedTrainerWorkersChain<L, K, V, I, C, I> create() {
        return (input, context) -> input;
    }

    default <O1> DistributedTrainerWorkersChain<L, K, V, I, C, O1> thenLocally(IgniteBiFunction<O, L, O1> localStep) {
        DistributedTrainerWorkersChain<L, K, V, O, C, O1> nextStep = (input, context) -> localStep.apply(input, context.localContext());
        return then(nextStep);
    }

    default <O1 extends Serializable, D, G> DistributedTrainerWorkersChain<L, K, V, I, C, O1> thenDistributed(
        IgniteFunction<O, G> remoteCtxExtractor,
        IgniteBiFunction<O, EntryAndContext<K, V, G>, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, O1>> distributedWorker,
        IgniteBiFunction<O, C, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> kf,
        IgniteConsumer<Map<GroupTrainerCacheKey, D>> distributedConsumer,
        IgniteBinaryOperator<O1> reducer) {
        DistributedTrainerWorkersChain<L, K, V, O, C, O1> nextStep = (input, context) -> {
            IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier = kf.apply(input, context);

            Ignite ignite = context.ignite();
            UUID trainingUUID = context.trainingUUID();
            String cacheName = context.cacheContext().cacheName();
            ClusterGroup grp = ignite.cluster().forDataNodes(cacheName);

            // Apply first argument locally because it is common for all nodes.
            IgniteSupplier<G> extractor = Functions.curry(remoteCtxExtractor).apply(input);
            IgniteFunction<EntryAndContext<K, V, G>, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, O1>> dWorker = Functions.curry(distributedWorker).apply(input);

            return ignite.compute(grp).execute(new GroupTrainerTask<>(trainingUUID, extractor, dWorker, distributedConsumer, keysSupplier, reducer, cacheName, input, ignite), null);
        };
        return then(nextStep);
    }

    default <O1 extends Serializable, D> DistributedTrainerWorkersChain<L, K, V, I, C, O1> thenDistributed(
        IgniteBiFunction<O, Map.Entry<GroupTrainerCacheKey<K>, V>, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, O1>> distributedWorker,
        IgniteBiFunction<O, C, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> kf,
        IgniteBinaryOperator<O1> reducer) {

        return thenDistributed(o -> null, (o, context) -> distributedWorker.apply(o, context.entry()), kf, map -> {}, reducer);
    }

    default <O1 extends Serializable> DistributedTrainerWorkersChain<L, K, V, I, C, O1> thenDistributedWrite(
        IgniteBiFunction<O, GroupTrainerCacheKey<K>, O1> distributedWorker,
        IgniteBiFunction<O, C, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> kf,
        IgniteBinaryOperator<O1> reducer) {


        return thenDistributed(o -> null, (o, context) -> distributedWorker.apply(o, context.entry()), kf, map -> {}, reducer);
    }

//    default <O1 extends Serializable, D> DistributedTrainerWorkersChain<L, V, K, I, C, O1> thenDistributedIntKeys(
//        IgniteBiFunction<O, Map.Entry<GroupTrainerCacheKey<K>, V>, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, O1>> distributedWorker,
//        IgniteBiFunction<O, C, IgniteSupplier<Stream<Integer>>> kf,
//        IgniteConsumer<Map<GroupTrainerCacheKey, D>> distributedConsumer,
//        IgniteBinaryOperator<O1> reducer) {
//        IgniteBiFunction<O, C, IgniteSupplier<Stream<GroupTrainerCacheKey<K>>>> kf1 = (o, c) -> {
//            IgniteSupplier<Stream<Integer>> keysSupplier1 = kf.apply(o, c);
//            Stream<GroupTrainerCacheKey<K>> stream = keysSupplier1.get().map(i -> new GroupTrainerCacheKey<K>(i, null, c.trainingUUID()));
//            return () -> stream;
//        };
//
//        return thenDistributed(distributedWorker, kf1, distributedConsumer, reducer);
//    }

    default DistributedTrainerWorkersChain<L, K, V, I, C, O> thenWhile(IgnitePredicate<O> cond, DistributedTrainerWorkersChain<L, K, V, O, C, O> chain) {
        DistributedTrainerWorkersChain<L, K, V, I, C, O> me = this;
        return (input, context) -> {
            O res = me.process(input, context);

            while (cond.apply(res))
                res = chain.process(res, context);

            return res;
        };
    }

    default <C1, O1> DistributedTrainerWorkersChain<L, K, V, I, C, O1> withOtherContext(IWorkersChain<O, C1, O1> newChain, C1 otherContext) {
        return (input, context) -> {
            O res = process(input, context);
            return newChain.process(res, otherContext);
        };
    }

    default <O1> DistributedTrainerWorkersChain<L, K, V, I, C, O1> then(DistributedTrainerWorkersChain<L, K, V, O, C, O1> next) {
        DistributedTrainerWorkersChain<L, K, V, I, C, O> me = this;
        return (input, context) -> {
            O myRes = me.process(input, context);
            return next.process(myRes, context);
        };
    }
}
