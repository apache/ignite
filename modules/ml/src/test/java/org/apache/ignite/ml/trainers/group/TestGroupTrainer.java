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

import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.trainers.group.chain.DC;
import org.apache.ignite.ml.trainers.group.chain.DistributedTrainerWorkersChain;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;

public class TestGroupTrainer extends GroupTrainer<TestGroupTrainerLocalContext, Double, Integer, Integer, Integer, Double, ConstModel<Integer>, SimpleDistributive, Void> {
    public TestGroupTrainer(Ignite ignite) {
        super(TestGroupTrainingCache.getOrCreate(ignite), ignite);
    }

    @Override protected TestGroupTrainerLocalContext initialLocalContext(SimpleDistributive data, UUID trainingUUID) {
        return new TestGroupTrainerLocalContext(data.iterCnt(), data.eachNumberCount(), data.limit(), trainingUUID);
    }

    @Override protected ResultAndUpdates<Integer> initGlobal(SimpleDistributive data, GroupTrainerCacheKey<Double> key) {
        int i = key.nodeLocalEntityIndex();
        UUID trainingUUID = key.trainingUUID();
        IgniteCache<GroupTrainerCacheKey<Double>, Integer> cache = TestGroupTrainingCache.getOrCreate(Ignition.localIgnite());

        int sum = i * data.eachNumberCount();

        ResultAndUpdates<Integer> res = ResultAndUpdates.of(sum);

        for (int j = 0; j < data.eachNumberCount(); j++)
            res.update(cache, new GroupTrainerCacheKey<>(i, (double)j, trainingUUID), i);

        return res;
    }

    @Override protected Integer reduceGlobalInitData(Integer data1, Integer data2) {
        return data1 + data2;
    }

    @Override protected Double processInitData(Integer data, TestGroupTrainerLocalContext locCtx) {
        return data.doubleValue();
    }

    @Override
    protected DistributedTrainerWorkersChain<TestGroupTrainerLocalContext,
        Double, Integer, Double, GroupTrainingContext<Double, Integer, TestGroupTrainerLocalContext>, Double> trainingLoopStep() {
        // TODO: here we should explicitly create variable because we cannot infer context type, think about it.
        DistributedTrainerWorkersChain<TestGroupTrainerLocalContext, Double, Integer, Double, GroupTrainingContext<Double, Integer, TestGroupTrainerLocalContext>, Double> chain = DC.
            create(new TestTrainingLoopStep());
        return chain.
            thenLocally((aDouble, context) -> {
            context.incCnt();
            return aDouble;
        });
    }

    @Override protected boolean shouldContinue(Double data, TestGroupTrainerLocalContext locCtx) {
        return locCtx.cnt() < locCtx.maxCnt();
    }

    @Override protected Void extractContextForModelCreation(Double data, TestGroupTrainerLocalContext locCtx) {
        // No context is needed.
        return null;
    }

    @Override
    protected Stream<GroupTrainerCacheKey<Double>> finalResultKeys(Double data, TestGroupTrainerLocalContext locCtx) {
        return TestGroupTrainingCache.allKeys(locCtx.limit(), locCtx.eachNumberCnt(), locCtx.trainingUUID());
    }

    @Override protected ResultAndUpdates<Integer> getFinalResults(Double data, TestGroupTrainerLocalContext locCtx,
        EntryAndContext<Double, Integer, Void> entryAndCtx) {
        Integer val = entryAndCtx.entry().getValue();
        return ResultAndUpdates.of(val % 2 == 0 ? val : 0);
    }

    @Override protected Integer defaultFinalResult() {
        // Default result is null;
        return null;
    }

    @Override protected Integer reduceFinalResults(Integer res1, Integer res2) {
        return res1 + res2;
    }

    @Override protected ConstModel<Integer> mapFinalResult(Integer res, TestGroupTrainerLocalContext locCtx) {
        return new ConstModel<>(res);
    }

    @Override protected void cleanup(TestGroupTrainerLocalContext locCtx) {
        Stream<GroupTrainerCacheKey<Double>> toRemote = TestGroupTrainingCache.allKeys(locCtx.limit(), locCtx.eachNumberCnt(), locCtx.trainingUUID());
        TestGroupTrainingCache.getOrCreate(ignite).removeAll(toRemote.collect(Collectors.toSet()));
    }
}
