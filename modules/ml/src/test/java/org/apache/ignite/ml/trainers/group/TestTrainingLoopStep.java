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

import java.util.stream.Stream;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.trainers.group.chain.DistributedStep;

public class TestTrainingLoopStep implements DistributedStep<TestGroupTrainerLocalContext, Double, Integer, Void, Double, Double> {
    @Override public Void extractRemoteContext(Double input, TestGroupTrainerLocalContext locCtx) {
        // No context is needed.
        return null;
    }

    @Override public ResultAndUpdates<Double> worker(Double input, TestGroupTrainerLocalContext locCtx,
        EntryAndContext<Double, Integer, Void> entryAndContext) {
        Integer oldVal = entryAndContext.entry().getValue();
        double v = oldVal * oldVal;
        ResultAndUpdates<Double> res = ResultAndUpdates.of(v);
        res.update(TestGroupTrainingCache.getOrCreate(Ignition.localIgnite()), entryAndContext.entry().getKey(), (int)v);
        return res;
    }

    @Override public IgniteSupplier<Stream<GroupTrainerCacheKey<Double>>> keysSupplier(Double input,
        TestGroupTrainerLocalContext locCtx) {
        return () -> TestGroupTrainingCache.allKeys(locCtx.limit(), locCtx.eachNumberCnt(), locCtx.trainingUUID());
    }

    @Override public Double identity() {
        return 0.0;
    }

    @Override public Double reduce(Double arg1, Double arg2) {
        return arg1 + arg2;
    }
}
