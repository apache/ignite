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
import java.util.stream.Stream;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.trainers.group.chain.DistributedEntryProcessingStep;

/** */
public class TestTrainingLoopStep implements DistributedEntryProcessingStep<TestGroupTrainerLocalContext,
    Double, Integer, Void, Double, Double> {
    /** {@inheritDoc} */
    @Override public IgniteSupplier<Void> remoteContextSupplier(Double input, TestGroupTrainerLocalContext locCtx) {
        // No context is needed.
        return () -> null;
    }

    /** {@inheritDoc} */
    @Override public IgniteFunction<EntryAndContext<Double, Integer, Void>, ResultAndUpdates<Double>> worker() {
        return entryAndContext -> {
            Integer oldVal = entryAndContext.entry().getValue();
            double v = oldVal * oldVal;
            ResultAndUpdates<Double> res = ResultAndUpdates.of(v);
            res.updateCache(TestGroupTrainingCache.getOrCreate(Ignition.localIgnite()),
                entryAndContext.entry().getKey(), (int)v);
            return res;
        };
    }

    /** {@inheritDoc} */
    @Override public IgniteSupplier<Stream<GroupTrainerCacheKey<Double>>> keys(Double input,
        TestGroupTrainerLocalContext locCtx) {
        // Copying here because otherwise locCtx will be serialized with supplier returned in result.
        int limit = locCtx.limit();
        int cnt = locCtx.eachNumberCnt();
        UUID uuid = locCtx.trainingUUID();

        return () -> TestGroupTrainingCache.allKeys(limit, cnt, uuid);
    }

    /** {@inheritDoc} */
    @Override public IgniteFunction<List<Double>, Double> reducer() {
        return doubles -> doubles.stream().mapToDouble(x -> x).sum();
    }
}
