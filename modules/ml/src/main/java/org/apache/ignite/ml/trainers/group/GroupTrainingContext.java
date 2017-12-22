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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.trainers.group.chain.CacheContext;
import org.apache.ignite.ml.trainers.group.chain.HasCacheContext;
import org.apache.ignite.ml.trainers.group.chain.HasLocalContext;
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

public class GroupTrainingContext<K, V, L extends HasTrainingUUID> implements HasCacheContext<GroupTrainerCacheKey<K>, V>, HasLocalContext<L> {
    private L localContext;
    private CacheContext<GroupTrainerCacheKey<K>, V> cacheContext;
    private Ignite ignite;

    public GroupTrainingContext(L locCtx, CacheContext<GroupTrainerCacheKey<K>, V> cacheCtx, Ignite ignite) {
        this.localContext = locCtx;
        this.cacheContext = cacheCtx;
        this.ignite = ignite;
    }

    public <K1, V1> GroupTrainingContext<K1, V1, L> withOtherCache(IgniteCache<GroupTrainerCacheKey<K1>, V1> otherCache) {
        CacheContext<GroupTrainerCacheKey<K1>, V1> newCtx = new CacheContext<>(otherCache);
        return new GroupTrainingContext<>(localContext, newCtx, ignite);
    }

    @Override public L localContext() {
        return localContext;
    }

    @Override public CacheContext<GroupTrainerCacheKey<K>, V> cacheContext() {
        return cacheContext;
    }

    @Override public Ignite ignite() {
        return ignite;
    }
}