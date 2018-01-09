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

package org.apache.ignite.ml.nn.trainers.distributed;

import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;

/**
 * Cache for distributed MLP.
 */
public class MLPCache {
    /**
     * Cache name.
     */
    public static String CACHE_NAME = "MLP_CACHE";

    /**
     * Affinity service for region projections cache.
     *
     * @return Affinity service for region projections cache.
     */
    public static Affinity<GroupTrainerCacheKey<Void>> affinity() {
        return Ignition.localIgnite().affinity(CACHE_NAME);
    }

    /**
     * Get or create region projections cache.
     *
     * @param ignite Ignite instance.
     * @return Region projections cache.
     */
    public static IgniteCache<GroupTrainerCacheKey<Void>, MLPGroupTrainingCacheValue> getOrCreate(Ignite ignite) {
        CacheConfiguration<GroupTrainerCacheKey<Void>, MLPGroupTrainingCacheValue> cfg = new CacheConfiguration<>();

        // Write to primary.
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        // Atomic transactions only.
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        // No copying of values.
        cfg.setCopyOnRead(false);

        // Cache is partitioned.
        cfg.setCacheMode(CacheMode.PARTITIONED);

        cfg.setBackups(0);

        cfg.setOnheapCacheEnabled(true);

        cfg.setName(CACHE_NAME);

        return ignite.getOrCreateCache(cfg);
    }

    /**
     * Get all keys of this cache for given parameters.
     *
     * @param trainingsCnt Parallel trainings count.
     * @param uuid Training UUID.
     * @return All keys of this cache for given parameters.
     */
    public static Stream<GroupTrainerCacheKey<Void>> allKeys(int trainingsCnt, UUID uuid) {
        return IntStream.range(0, trainingsCnt).mapToObj(i -> new GroupTrainerCacheKey<Void>(i, null, uuid));
    }
}
