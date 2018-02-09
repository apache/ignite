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

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;

/** */
public class TestGroupTrainingCache {
    /** */
    public static String CACHE_NAME = "TEST_GROUP_TRAINING_CACHE";

    /** */
    public static IgniteCache<GroupTrainerCacheKey<Double>, Integer> getOrCreate(Ignite ignite) {
        CacheConfiguration<GroupTrainerCacheKey<Double>, Integer> cfg = new CacheConfiguration<>();

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

    /** */
    public static Stream<GroupTrainerCacheKey<Double>> allKeys(int limit, int eachNumberCnt, UUID trainingUUID) {
        GroupTrainerCacheKey<Double>[] a =new GroupTrainerCacheKey[limit * eachNumberCnt];

        for (int num = 0; num < limit; num++)
            for (int i = 0; i < eachNumberCnt; i++)
                a[num * eachNumberCnt + i] = new GroupTrainerCacheKey<>(num, (double)i, trainingUUID);

        return Arrays.stream(a);
    }
}
