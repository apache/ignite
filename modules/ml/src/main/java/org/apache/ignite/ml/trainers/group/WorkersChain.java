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

package org.apache.ignite.ml.trainers.group;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteConsumer;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;

public class WorkersChain<I, L, G, O> {
    private List<ChainStep> steps;

    public WorkersChain() {
        steps = new LinkedList<>();
    }

    private WorkersChain(List<ChainStep> steps) {
        this.steps = steps;
    }

    public <O1> WorkersChain<O, L, G, O1> thenLocally(IgniteBiFunction<O, L, Optional<O1>> localWorker) {
        return withNewStep(ChainStep.localStep(localWorker));
    }

    public <O1, D> WorkersChain<O, L, G, O1> thenGlobally(IgniteBiFunction<Map.Entry<GroupTrainerCacheKey, G>, O, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, O1>> distributedWorker,
        IgniteFunction<O, Stream<Integer>> kf,
        IgniteConsumer<Map<GroupTrainerCacheKey, D>> distributedConsumer,
        IgniteBinaryOperator<O1> reducer) {
        return withNewStep(ChainStep.distributedStep(distributedWorker, kf, distributedConsumer, reducer));
    }

    private <O1> WorkersChain<O, L, G, O1> withNewStep(ChainStep<O, ?, ?, ?, O1> step) {
        List<ChainStep> newSteps = new LinkedList<>(steps);
        newSteps.add(step);

        return new WorkersChain<>(newSteps);
    }

    public O process(UUID trainingUUID, String cacheName, I data, Ignite ignite) {
        Object d = data;
        Object res = null;
        for (ChainStep step : steps) {
            IgniteSupplier<Stream<Integer>> keysSupplier = (IgniteSupplier<Stream<Integer>>)step.kf.apply(d);
            res = ignite.compute(ignite.cluster().forDataNodes(cacheName)).execute(new GroupTrainerTask<>(trainingUUID, step.distributedWorker, keysSupplier, step.reducer, cacheName, d, ignite), null);
        }

        return (O)res;
    }

    private static class ChainStep<I1, L, G, D, O1> {
        boolean isLocal;
        private IgniteBiFunction<I1, L, Optional<O1>> localWorker;

        private IgniteBiFunction<Map.Entry<GroupTrainerCacheKey, G>, I1, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, O1>> distributedWorker;
        private IgniteFunction<I1, Stream<Integer>> kf;
        private IgniteConsumer<Map<GroupTrainerCacheKey, D>> distributedConsumer;
        private IgniteBinaryOperator<O1> reducer;

        private ChainStep(boolean isLocal,
            IgniteBiFunction<I1, L, Optional<O1>> localWorker,
            IgniteFunction<I1, Stream<Integer>> kf,
            IgniteBiFunction<Map.Entry<GroupTrainerCacheKey, G>, I1, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, O1>> distributedWorker,
            IgniteConsumer<Map<GroupTrainerCacheKey, D>> distributedConsumer,
            IgniteBinaryOperator<O1> reducer) {
            this.isLocal = isLocal;
            this.localWorker = localWorker;
            this.kf = kf;
            this.distributedWorker = distributedWorker;
            this.distributedConsumer = distributedConsumer;
            this.reducer = reducer;
        }

        public static <I, L, O> ChainStep<I, L, ?, ?, O> localStep(IgniteBiFunction<I, L, Optional<O>> localStep) {
            return new ChainStep<>(true, localStep, null, null, null, null);
        }

        public static <I, G, D, O> ChainStep<I, ?, G, D, O> distributedStep(IgniteBiFunction<Map.Entry<GroupTrainerCacheKey, G>, I, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, D>, O>> distributedWorker,
            IgniteFunction<I, Stream<Integer>> kf,
            IgniteConsumer<Map<GroupTrainerCacheKey, D>> distributedConsumer,
            IgniteBinaryOperator<O> reducer) {
            return new ChainStep<>(false, null, kf, distributedWorker, distributedConsumer, reducer);
        }
    }
}