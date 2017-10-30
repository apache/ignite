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
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;

public class WorkersChain<I, G, O> {
    private List<ChainStep> steps;

    public WorkersChain(int localEntitiesForNodeCnt) {
        steps = new LinkedList<>();
    }

    private WorkersChain(List<ChainStep> steps) {
        this.steps = steps;
    }

    public <O1> WorkersChain<O, G, O1> andThen(ChainStep<O, G, O1> chainStep) {
        List<ChainStep> newSteps = new LinkedList<>();
        newSteps.add(chainStep);

        return new WorkersChain<>(newSteps);
    }

    public O process(UUID trainingUUID, String cacheName, I data, Ignite ignite) {
        Object d = data;
        for (ChainStep step : steps) {
            step.kf.apply(d);
            Object res = ignite.compute(ignite.cluster().forDataNodes(cacheName)).execute(new GroupTrainerTask<>(trainingUUID, step.f, step.reducer, cacheName, d), null);

        }
    }

    private static class ChainStep<I1, G, O1> {
        private IgniteFunction<O1, Stream<Integer>> kf;
        private IgniteBiFunction<Map.Entry<GroupTrainerCacheKey, G>, I1, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, G>, O1>> f;
        private IgniteBinaryOperator<O1> reducer;

        private ChainStep(IgniteFunction<O1, Stream<Integer>> kf,
            IgniteBiFunction<Map.Entry<GroupTrainerCacheKey, G>, I1, IgniteBiTuple<Map.Entry<GroupTrainerCacheKey, G>, O1>> f,
            IgniteBinaryOperator<O1> reducer) {
            this.kf = kf;
            this.f = f;
            this.reducer = reducer;
        }

//        public static <X, Y> ChainStep<X, Y> of(IgniteFunction<X, Y> f) {
//            return new ChainStep<X, Y>(f, );
//        }
    }
}