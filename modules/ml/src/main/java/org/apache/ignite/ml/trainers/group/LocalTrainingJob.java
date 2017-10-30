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
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.util.StreamUtil;

public class LocalTrainingJob<S, U extends Serializable, G> implements ComputeJob {
    private IgniteBiFunction<Cache.Entry<GroupTrainerCacheKey, G>, S, IgniteBiTuple<Cache.Entry<GroupTrainerCacheKey, G>, U>> worker;
    private UUID trainingUUID;
    private String cacheName;
    private Ignite ignite;
    IgniteCache<GroupTrainerCacheKey, G> cache;
    S data;

    public LocalTrainingJob(IgniteBiFunction<Cache.Entry<GroupTrainerCacheKey, G>, S, IgniteBiTuple<Cache.Entry<GroupTrainerCacheKey, G>, U>> worker,
        UUID trainingUUID, String cacheName,
        S data) {
        this.worker = worker;
        this.trainingUUID = trainingUUID;
        this.cacheName = cacheName;
        this.data = data;

        ignite = Ignition.localIgnite();
        cache = ignite.getOrCreateCache(cacheName);
    }

    @Override public void cancel() {

    }

    @Override public Object execute() throws IgniteException {
        List<IgniteBiTuple<Cache.Entry<GroupTrainerCacheKey, G>, U>> res = selectLocalEntries().
            parallel().
            map(entry -> worker.apply(entry, data)).collect(Collectors.toList());
        res
    }

    // TODO: do it more optimally.
    private Stream<Cache.Entry<GroupTrainerCacheKey, G>> selectLocalEntries() {
        return StreamUtil.fromIterator(cache.localEntries().iterator()).filter(entry -> entry.getKey().trainingUUID().equals(trainingUUID));
    }
}
