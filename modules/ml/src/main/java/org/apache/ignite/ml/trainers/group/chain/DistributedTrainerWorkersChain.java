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
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteConsumer;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;
import org.apache.ignite.ml.trainers.group.GroupTrainerTask;

public interface DistributedTrainerWorkersChain<L, G, I, C extends HasCacheContext<GroupTrainerCacheKey, G> & HasLocalContext<L> & HasTrainingUUID, O> extends BaseWorkersChain<I, C, O> {
    default DistributedTrainerWorkersChain<L, G, I, C, I> create() {
        return (input, context) -> input;
    }

    default <O1> DistributedTrainerWorkersChain<L, G, I, C, O1> thenLocally(IgniteBiFunction<O, L, O1> localStep) {
        DistributedTrainerWorkersChain<L, G, O, C, O1> nextStep = (input, context) -> localStep.apply(input, context.localContext());
        return then(nextStep);
    }

    default <O1 extends Serializable, D> DistributedTrainerWorkersChain<L, G, I, C, O1> thenDistributed(
        IgniteBiFunction<Map.Entry<GroupTrainerCacheKey, G>, O, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, O1>> distributedWorker,
        IgniteBiFunction<O, C, IgniteSupplier<Stream<Integer>>> kf,
        IgniteConsumer<Map<GroupTrainerCacheKey, D>> distributedConsumer,
        IgniteBinaryOperator<O1> reducer) {
        DistributedTrainerWorkersChain<L, G, O, C, O1> nextStep = (input, context) -> {
            IgniteSupplier<Stream<Integer>> keysSupplier = kf.apply(input, context);

            Ignite ignite = context.ignite();
            UUID trainingUUID = context.trainingUUID();
            String cacheName = context.cacheContext().cacheName();
            ClusterGroup grp = ignite.cluster().forDataNodes(cacheName);

            return ignite.compute(grp).execute(new GroupTrainerTask<>(trainingUUID, distributedWorker, distributedConsumer, keysSupplier, reducer, cacheName, input, ignite), null);
        };
        return then(nextStep);
    }

    default <C1, O1> DistributedTrainerWorkersChain<L, G, I, C, O1> withOtherContext(IWorkersChain<O, C1, O1> newChain, C1 otherContext) {
        return (input, context) -> {
            O res = process(input, context);
            return newChain.process(res, otherContext);
        };
    }

    default <O1> DistributedTrainerWorkersChain<L, G, I, C, O1> then(DistributedTrainerWorkersChain<L, G, O, C, O1> next) {
        DistributedTrainerWorkersChain<L, G, I, C, O> me = this;
        return (input, context) -> {
            O myRes = me.process(input, context);
            return next.process(myRes, context);
        };
    }
}
