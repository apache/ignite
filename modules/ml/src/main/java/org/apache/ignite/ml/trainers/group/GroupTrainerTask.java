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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteUncurriedBiFunction;
import org.jetbrains.annotations.Nullable;

public class GroupTrainerTask<S, G, U extends Serializable> extends ComputeTaskAdapter<Void, U> {
    private UUID trainingUUID;
    private IgniteBiFunction<Cache.Entry<GroupTrainerCacheKey, G>, S, IgniteBiTuple<Cache.Entry<GroupTrainerCacheKey, G>, U>> worker;
    // TODO: Also use this reducer on local steps.
    private IgniteBinaryOperator<U> reducer;
    private String cacheName;
    private S data;

    public GroupTrainerTask(UUID trainingUUID,
        IgniteBiFunction<Cache.Entry<GroupTrainerCacheKey, G>, S, IgniteBiTuple<Cache.Entry<GroupTrainerCacheKey, G>, U>> worker,
        String cacheName,
        S data) {
        this.trainingUUID = trainingUUID;
        this.worker = worker;
        this.cacheName = cacheName;
        this.data = data;
    }

    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Void arg) throws IgniteException {
        Map<ComputeJob, ClusterNode> res = new HashMap<>();

        for (ClusterNode node : subgrid)
            res.put(new LocalTrainingJob<>(worker, trainingUUID, cacheName, data), node);

        return res;
    }

    @Override
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        return super.result(res, rcvd);
    }

    @Nullable @Override
    public U reduce(List<ComputeJobResult> results) throws IgniteException {
        return results.stream().map(res -> (U)res.getData()).reduce();
    }
}
