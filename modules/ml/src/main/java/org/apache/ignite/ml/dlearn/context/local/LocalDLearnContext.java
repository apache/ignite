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

package org.apache.ignite.ml.dlearn.context.local;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.ml.dlearn.DLearnContext;
import org.apache.ignite.ml.dlearn.DLearnPartitionFactory;
import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;
import org.apache.ignite.ml.dlearn.utils.DLearnContextPartitionKey;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;

/**
 * Learning context based on a local on-heap storage.
 *
 * @param <P> type of learning context partition
 */
public class LocalDLearnContext<P> implements DLearnContext<P> {
    /** */
    private final Map<DLearnContextPartitionKey, Object> learningCtxMap;

    /** */
    private final DLearnPartitionFactory<P> partFactory;

    /** */
    private final UUID learningCtxId;

    /** */
    private final int partitions;

    /** */
    public LocalDLearnContext(Map<DLearnContextPartitionKey, Object> learningCtxMap,
        DLearnPartitionFactory<P> partFactory, UUID learningCtxId, int partitions) {
        this.learningCtxMap = learningCtxMap;
        this.partFactory = partFactory;
        this.learningCtxId = learningCtxId;
        this.partitions = partitions;
    }

    /** */
    @Override public <R> R compute(IgniteBiFunction<P, Integer, R> mapper, IgniteBinaryOperator<R> reducer) {
        R res = null;
        for (int partIdx = 0; partIdx < partitions; partIdx++) {
            DLearnPartitionStorage storage = new LocalDLearnPartitionStorage(learningCtxMap, learningCtxId, partIdx);
            P part = partFactory.createPartition(storage);
            R partRes = mapper.apply(part, partIdx);
            res = reducer.apply(res, partRes);
        }
        return res;
    }

    /** */
    @Override public void compute(IgniteBiConsumer<P, Integer> mapper) {
        for (int partIdx = 0; partIdx < partitions; partIdx++) {
            DLearnPartitionStorage storage = new LocalDLearnPartitionStorage(learningCtxMap, learningCtxId, partIdx);
            P part = partFactory.createPartition(storage);
            mapper.accept(part, partIdx);
        }
    }

    /** */
    @Override public <T> DLearnContext<T> transform(IgniteBiConsumer<P, T> transformer,
        DLearnPartitionFactory<T> partFactory) {
        UUID newLearningCtxId = UUID.randomUUID();

        compute((part, partIdx) -> {
            DLearnPartitionStorage newStorage = new LocalDLearnPartitionStorage(learningCtxMap, newLearningCtxId, partIdx);
            T newPart = partFactory.createPartition(newStorage);
            transformer.accept(part, newPart);
        });

        return new LocalDLearnContext<>(learningCtxMap, partFactory, newLearningCtxId, partitions);
    }
}
