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
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;

/**
 * Base job for group training.
 * It's purpose is to apply worker to each element (cache key or cache entry) of given cache specified
 * by keySupplier. Worker produces {@link ResultAndUpdates} object which contains 'side effects' which are updates
 * needed to apply to caches and computation result.
 * After we get all {@link ResultAndUpdates} we merge all 'update' parts of them for each node
 * and apply them on corresponding node, also we reduce all 'result' by some given reducer.
 *
 * @param <K> Type of keys of cache used for group trainer.
 * @param <V> Type of values of cache used for group trainer.
 * @param <T> Type of elements to which workers are applier.
 * @param <R> Type of result of worker.
 */
public abstract class BaseLocalProcessorJob<K, V, T, R extends Serializable> implements ComputeJob {
    /**
     * UUID of group training.
     */
    protected UUID trainingUUID;

    /**
     * Worker.
     */
    protected IgniteFunction<T, ResultAndUpdates<R>> worker;

    /**
     * Supplier of keys determining elements to which worker should be applied.
     */
    protected IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keySupplier;

    /**
     * Operator used to reduce results from worker.
     */
    protected IgniteBinaryOperator<R> reducer;

    /**
     * Identity for reducer.
     */
    protected final R identity;

    /**
     * Name of cache used for training.
     */
    protected String cacheName;

    /**
     * Construct instance of this class with given arguments.
     *
     * @param worker Worker.
     * @param keySupplier Supplier of keys.
     * @param reducer Reducer.
     * @param identity Identity for reducer.
     * @param trainingUUID UUID of training.
     * @param cacheName Name of cache used for training.
     */
    public BaseLocalProcessorJob(
        IgniteFunction<T, ResultAndUpdates<R>> worker,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keySupplier,
        IgniteBinaryOperator<R> reducer,
        R identity,
        UUID trainingUUID, String cacheName) {
        this.worker = worker;
        this.keySupplier = keySupplier;
        this.identity = identity;
        this.reducer = reducer;
        this.trainingUUID = trainingUUID;
        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        // NO-OP.
    }

    /** {@inheritDoc} */
    @Override public R execute() throws IgniteException {
        List<ResultAndUpdates<R>> resultsAndUpdates = toProcess().
            map(worker).
            collect(Collectors.toList());

        ResultAndUpdates<R> totalRes = ResultAndUpdates.sum(reducer, identity, resultsAndUpdates);

        totalRes.applyUpdates(ignite());

        return totalRes.result();
    }

    /**
     * Get stream of elements to process.
     *
     * @return Stream of elements to process.
     */
    protected abstract Stream<T> toProcess();

    /**
     * Ignite instance.
     *
     * @return Ignite instance.
     */
    protected static Ignite ignite() {
        return Ignition.localIgnite();
    }

    /**
     * Get cache used for training.
     *
     * @return Cache used for training.
     */
    protected IgniteCache<GroupTrainerCacheKey<K>, V> cache() {
        return ignite().getOrCreateCache(cacheName);
    }

    /**
     * Get affinity function for cache used in group training.
     *
     * @return Affinity function for cache used in group training.
     */
    protected Affinity<GroupTrainerCacheKey> affinity() {
        return ignite().affinity(cacheName);
    }
}
