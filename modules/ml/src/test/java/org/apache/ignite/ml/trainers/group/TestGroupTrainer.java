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

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.Chains;
import org.apache.ignite.ml.trainers.group.chain.ComputationsChain;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;

/**
 * Test group trainer.
 */
public class TestGroupTrainer extends GroupTrainer<TestGroupTrainerLocalContext, Double, Integer, Integer, Integer,
    Double, ConstModel<Integer>, SimpleGroupTrainerInput, Void> {
    /**
     * Construct instance of this class with given parameters.
     *
     * @param ignite Ignite instance.
     */
    public TestGroupTrainer(Ignite ignite) {
        super(TestGroupTrainingCache.getOrCreate(ignite), ignite);
    }

    /** {@inheritDoc} */
    @Override protected TestGroupTrainerLocalContext initialLocalContext(SimpleGroupTrainerInput data,
        UUID trainingUUID) {
        return new TestGroupTrainerLocalContext(data.iterCnt(), data.eachNumberCount(), data.limit(), trainingUUID);
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<GroupTrainerCacheKey<Double>, ResultAndUpdates<Integer>> distributedInitializer(
        SimpleGroupTrainerInput data) {
        return key -> {
            long i = key.nodeLocalEntityIndex();
            UUID trainingUUID = key.trainingUUID();
            IgniteCache<GroupTrainerCacheKey<Double>, Integer> cache
                = TestGroupTrainingCache.getOrCreate(Ignition.localIgnite());

            long sum = i * data.eachNumberCount();

            ResultAndUpdates<Integer> res = ResultAndUpdates.of((int)sum);

            for (int j = 0; j < data.eachNumberCount(); j++)
                res.updateCache(cache, new GroupTrainerCacheKey<>(i, (double)j, trainingUUID), (int)i);

            return res;
        };
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<List<Integer>, Integer> reduceDistributedInitData() {
        return id -> id.stream().mapToInt(x -> x).sum();
    }

    /** {@inheritDoc} */
    @Override protected Double locallyProcessInitData(Integer data, TestGroupTrainerLocalContext locCtx) {
        return data.doubleValue();
    }

    /** {@inheritDoc} */
    @Override protected ComputationsChain<TestGroupTrainerLocalContext,
        Double, Integer, Double, Double> trainingLoopStep() {
        // TODO: here we should explicitly create variable because we cannot infer context type, think about it.
        ComputationsChain<TestGroupTrainerLocalContext, Double, Integer, Double, Double> chain = Chains.
            create(new TestTrainingLoopStep());
        return chain.
            thenLocally((aDouble, context) -> {
                context.incCnt();
                return aDouble;
            });
    }

    /** {@inheritDoc} */
    @Override protected boolean shouldContinue(Double data, TestGroupTrainerLocalContext locCtx) {
        return locCtx.cnt() < locCtx.maxCnt();
    }

    /** {@inheritDoc} */
    @Override protected IgniteSupplier<Void> extractContextForFinalResultCreation(Double data,
        TestGroupTrainerLocalContext locCtx) {
        // No context is needed.
        return () -> null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteSupplier<Stream<GroupTrainerCacheKey<Double>>> finalResultKeys(Double data,
        TestGroupTrainerLocalContext locCtx) {
        int limit = locCtx.limit();
        int cnt = locCtx.eachNumberCnt();
        UUID uuid = locCtx.trainingUUID();

        return () -> TestGroupTrainingCache.allKeys(limit, cnt, uuid);
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<EntryAndContext<Double, Integer, Void>,
        ResultAndUpdates<Integer>> finalResultsExtractor() {
        return entryAndCtx -> {
            Integer val = entryAndCtx.entry().getValue();
            return ResultAndUpdates.of(val % 2 == 0 ? val : 0);
        };
    }

    /** {@inheritDoc} */
    @Override protected IgniteFunction<List<Integer>, Integer> finalResultsReducer() {
        return id -> id.stream().mapToInt(x -> x).sum();
    }

    /** {@inheritDoc} */
    @Override protected ConstModel<Integer> mapFinalResult(Integer res, TestGroupTrainerLocalContext locCtx) {
        return new ConstModel<>(res);
    }

    /** {@inheritDoc} */
    @Override protected void cleanup(TestGroupTrainerLocalContext locCtx) {
        Stream<GroupTrainerCacheKey<Double>> toRemote = TestGroupTrainingCache.allKeys(locCtx.limit(),
            locCtx.eachNumberCnt(), locCtx.trainingUUID());

        TestGroupTrainingCache.getOrCreate(ignite).removeAll(toRemote.collect(Collectors.toSet()));
    }
}
