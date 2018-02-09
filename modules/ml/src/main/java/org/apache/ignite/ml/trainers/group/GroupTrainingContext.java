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
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

/**
 * Context for group training.
 *
 * @param <K> Type of keys of cache used for group training.
 * @param <V> Type of values of cache used for group training.
 * @param <L> Type of local context used for training.
 */
public class GroupTrainingContext<K, V, L extends HasTrainingUUID> {
    /**
     * Local context.
     */
    private L locCtx;

    /**
     * Cache used for training.
     */
    private IgniteCache<GroupTrainerCacheKey<K>, V> cache;

    /**
     * Ignite instance.
     */
    private Ignite ignite;

    /**
     * Construct instance of this class.
     *
     * @param locCtx Local context.
     * @param cache Information about cache used for training.
     * @param ignite Ignite instance.
     */
    public GroupTrainingContext(L locCtx, IgniteCache<GroupTrainerCacheKey<K>, V> cache, Ignite ignite) {
        this.locCtx = locCtx;
        this.cache = cache;
        this.ignite = ignite;
    }

    /**
     * Construct new training context with same parameters but with new cache.
     *
     * @param newCache New cache.
     * @param <K1> Type of keys of new cache.
     * @param <V1> Type of values of new cache.
     * @return New training context with same parameters but with new cache.
     */
    public <K1, V1> GroupTrainingContext<K1, V1, L> withCache(IgniteCache<GroupTrainerCacheKey<K1>, V1> newCache) {
        return new GroupTrainingContext<>(locCtx, newCache, ignite);
    }

    /**
     * Get local context.
     *
     * @return Local context.
     */
    public L localContext() {
        return locCtx;
    }

    /**
     * Get cache used for training.
     *
     * @return Cache used for training.
     */
    public IgniteCache<GroupTrainerCacheKey<K>, V> cache() {
        return cache;
    }

    /**
     * Get ignite instance.
     *
     * @return Ignite instance.
     */
    public Ignite ignite() {
        return ignite;
    }
}