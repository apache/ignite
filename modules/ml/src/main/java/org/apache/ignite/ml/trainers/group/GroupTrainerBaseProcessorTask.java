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
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
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
import org.jetbrains.annotations.Nullable;

/**
 * Base task for group trainer.
 *
 * @param <K> Type of cache keys of cache used for training.
 * @param <V> Type of cache values of cache used for training.
 * @param <C> Type of context (common part of data needed for computation).
 * @param <T> Type of arguments of workers.
 * @param <R> Type of computation result.
 */
public abstract class GroupTrainerBaseProcessorTask<K, V, C, T, R extends Serializable> extends ComputeTaskAdapter<Void, R> {
    /**
     * Context supplier.
     */
    protected final IgniteSupplier<C> ctxSupplier;

    /**
     * UUID of training.
     */
    protected final UUID trainingUUID;

    /**
     * Worker.
     */
    protected IgniteFunction<T, ResultAndUpdates<R>> worker;

    /**
     * Reducer used for reducing of computations on specified keys.
     */
    protected final IgniteBinaryOperator<R> reducer;

    /**
     * Identity for reducer.
     */
    protected final R identity;

    /**
     * Name of cache on which training is done.
     */
    protected final String cacheName;

    /**
     * Supplier of keys on which worker should be executed.
     */
    protected final IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier;

    /**
     * Ignite instance.
     */
    protected final Ignite ignite;

    /**
     * Construct an instance of this class with specified parameters.
     *
     * @param trainingUUID UUID of training.
     * @param ctxSupplier Supplier of context.
     * @param worker Function calculated on each of specified keys.
     * @param keysSupplier Supplier of keys on which training is done.
     * @param reducer Reducer used for reducing results of computation performed on each of specified keys.
     * @param identity Identity for reducer.
     * @param cacheName Name of cache on which training is done.
     * @param ignite Ignite instance.
     */
    public GroupTrainerBaseProcessorTask(UUID trainingUUID,
        IgniteSupplier<C> ctxSupplier,
        IgniteFunction<T, ResultAndUpdates<R>> worker,
        IgniteSupplier<Stream<GroupTrainerCacheKey<K>>> keysSupplier,
        IgniteBinaryOperator<R> reducer, R identity,
        String cacheName,
        Ignite ignite) {
        this.trainingUUID = trainingUUID;
        this.ctxSupplier = ctxSupplier;
        this.worker = worker;
        this.keysSupplier = keysSupplier;
        this.identity = identity;
        this.reducer = reducer;
        this.cacheName = cacheName;
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Void arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> res = new HashMap<>();

        for (ClusterNode node : subgrid) {
            BaseLocalProcessorJob<K, V, T, R> job = createJob();
            res.put(job, node);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res,
        List<ComputeJobResult> rcvd) throws IgniteException {
        return super.result(res, rcvd);
    }

    /** {@inheritDoc} */
    @Nullable @Override public R reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.stream().map(res -> (R)res.getData()).filter(Objects::nonNull).reduce(reducer).orElse(identity);
    }

    /**
     * Create job for execution on subgrid.
     *
     * @return Job for execution on subgrid.
     */
    protected abstract BaseLocalProcessorJob<K, V, T, R> createJob();

    /**
     * Get affinity function of cache on which training is done.
     *
     * @return Affinity function of cache on which training is done.
     */
    protected Affinity<GroupTrainerCacheKey> affinity() {
        return ignite.affinity(cacheName);
    }
}
