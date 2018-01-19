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
import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformer;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;

/**
 * Learning context based on a local on-heap storage.
 *
 * @param <P> type of learning context partition
 */
public class LocalDLearnContext<P extends AutoCloseable> implements DLearnContext<P> {
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

    /** {@inheritDoc} */
    @Override public <R> R compute(IgniteBiFunction<P, Integer, R> mapper, IgniteBinaryOperator<R> reducer,
        R identity) {
        R res = identity;
        for (int partIdx = 0; partIdx < partitions; partIdx++) {
            DLearnPartitionStorage storage = new LocalDLearnPartitionStorage(learningCtxMap, learningCtxId, partIdx);

            P part = partFactory.createPartition(storage);
            R partRes = mapper.apply(part, partIdx);

            res = reducer.apply(res, partRes);
        }
        return res;
    }

    /** {@inheritDoc} */
    @Override public void compute(IgniteBiConsumer<P, Integer> mapper) {
        for (int partIdx = 0; partIdx < partitions; partIdx++) {
            DLearnPartitionStorage storage = new LocalDLearnPartitionStorage(learningCtxMap, learningCtxId, partIdx);

            P part = partFactory.createPartition(storage);

            mapper.accept(part, partIdx);
        }
    }

    /** {@inheritDoc} */
    @Override public <T extends AutoCloseable, C extends DLearnContext<T>> C transform(
        DLearnContextTransformer<P, T, C> transformer) {
        UUID newLearnCtxId = UUID.randomUUID();

        compute((part, partIdx) -> {
            DLearnPartitionStorage newStorage = new LocalDLearnPartitionStorage(learningCtxMap, newLearnCtxId, partIdx);

            T newPart = transformer.createPartition(newStorage);

            transformer.transform(part, newPart);
        });

        DLearnContext<T> newCtx = new LocalDLearnContext<>(learningCtxMap, transformer, newLearnCtxId, partitions);

        return transformer.wrapContext(newCtx);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        compute(this::closePartition);
    }

    /** */
    private void closePartition(P part) {
        try {
            part.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    public Map<DLearnContextPartitionKey, Object> getLearningCtxMap() {
        return learningCtxMap;
    }
}
