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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.trainers.group.chain.EntryAndContext;
import org.apache.ignite.ml.trainers.group.chain.KeyAndContext;
import org.jetbrains.annotations.Nullable;

public class GroupTrainerKeysProcessorTask<K, S, V, G, U extends Serializable> extends GroupTrainerBaseProcessorTask<K, S, V, G, KeyAndContext<K, G>, U> {
    public GroupTrainerKeysProcessorTask(UUID trainingUUID,
        IgniteSupplier<G> ctxExtractor,
        IgniteFunction<KeyAndContext<K, G>, ResultAndUpdates<U>> remoteWorker,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier,
        U identity,
        IgniteBinaryOperator<U> reducer,
        String cacheName,
        S data,
        Ignite ignite) {
        super(trainingUUID, ctxExtractor, remoteWorker, keysSupplier, identity, reducer, cacheName, data, ignite);
    }

    @Override protected BaseLocalProcessorJob<K, V, KeyAndContext<K, G>, U> createJob() {
        return new LocalKeysProcessorJob<>(contextExtractor, worker, keysSupplier, identity, reducer, trainingUUID, cacheName);
    }
}
