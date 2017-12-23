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
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;

public abstract class BaseLocalProcessorJob<K, V, T, O extends Serializable> implements ComputeJob {
    protected final O identity;
    protected IgniteFunction<T, ResultAndUpdates<O>> worker;
    protected UUID trainingUUID;
    protected String cacheName;
    protected IgniteCache<GroupTrainerCacheKey<K>, V> cache;
    protected IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keySupplier;
    protected IgniteBinaryOperator<O> reducer;

    public BaseLocalProcessorJob(
        IgniteFunction<T, ResultAndUpdates<O>> worker,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keySupplier,
        O identity,
        IgniteBinaryOperator<O> reducer,
        UUID trainingUUID, String cacheName) {
        this.worker = worker;
        this.keySupplier = keySupplier;
        this.identity = identity;
        this.reducer = reducer;
        this.trainingUUID = trainingUUID;
        this.cacheName = cacheName;
    }

    @Override public void cancel() {

    }

    @Override public O execute() throws IgniteException {
        cache = ignite().getOrCreateCache(cacheName);

        List<ResultAndUpdates<O>> resultsAndUpdates = toProcess().
            map(worker::apply).
            collect(Collectors.toList());

        ResultAndUpdates<O> totalRes = ResultAndUpdates.sum(reducer, identity, resultsAndUpdates);

        totalRes.applyUpdates(ignite());

        return totalRes.result();
    }

    protected abstract Stream<T> toProcess();

    protected static Ignite ignite() {
        return Ignition.localIgnite();
    }
}
