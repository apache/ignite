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

package org.apache.ignite.ml.trees.trainers.columnbased.caches;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer;

/**
 * Class for working with cache used for storing of best splits during training with {@link ColumnDecisionTreeTrainer}.
 */
public class SplitCache {
    /** Name of splits cache. */
    public static final String CACHE_NAME = "COLUMN_DECISION_TREE_TRAINER_SPLIT_CACHE_NAME";

    /**
     * Class used for keys in the splits cache.
     */
    public static class SplitKey {
        /** UUID of current training. */
        private final UUID trainingUUID;

        /** Affinity key of input data. */
        @AffinityKeyMapped
        private final Object parentColKey;

        /** Index of feature by which the split is made. */
        private final int featureIdx;

        /**
         * Construct SplitKey.
         *
         * @param trainingUUID UUID of the training.
         * @param parentColKey Affinity key used to ensure that cache entry for given feature will be on the same node
         * as column with that feature in input.
         * @param featureIdx Feature index.
         */
        public SplitKey(UUID trainingUUID, Object parentColKey, int featureIdx) {
            this.trainingUUID = trainingUUID;
            this.featureIdx = featureIdx;
            this.parentColKey = parentColKey;
        }

        /** Get UUID of current training. */
        public UUID trainingUUID() {
            return trainingUUID;
        }

        /**
         * Get feature index.
         *
         * @return Feature index.
         */
        public int featureIdx() {
            return featureIdx;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            SplitKey splitKey = (SplitKey)o;

            if (featureIdx != splitKey.featureIdx)
                return false;
            return trainingUUID != null ? trainingUUID.equals(splitKey.trainingUUID) : splitKey.trainingUUID == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = trainingUUID != null ? trainingUUID.hashCode() : 0;
            res = 31 * res + featureIdx;
            return res;
        }
    }

    /**
     * Construct the key for splits cache.
     *
     * @param featureIdx Feature index.
     * @param parentColKey Affinity key used to ensure that cache entry for given feature will be on the same node as
     * column with that feature in input.
     * @param uuid UUID of current training.
     * @return Key for splits cache.
     */
    public static SplitKey key(int featureIdx, Object parentColKey, UUID uuid) {
        return new SplitKey(uuid, parentColKey, featureIdx);
    }

    /**
     * Get or create splits cache.
     *
     * @param ignite Ignite instance.
     * @return Splits cache.
     */
    public static IgniteCache<SplitKey, IgniteBiTuple<Integer, Double>> getOrCreate(Ignite ignite) {
        CacheConfiguration<SplitKey, IgniteBiTuple<Integer, Double>> cfg = new CacheConfiguration<>();

        // Write to primary.
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        // Atomic transactions only.
        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        // No eviction.
        cfg.setEvictionPolicy(null);

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
     * Affinity function used in splits cache.
     *
     * @return Affinity function used in splits cache.
     */
    public static Affinity<SplitKey> affinity() {
        return Ignition.localIgnite().affinity(CACHE_NAME);
    }

    /**
     * Returns local entries for keys corresponding to {@code featureIndexes}.
     *
     * @param featureIndexes Index of features.
     * @param affinity Affinity function.
     * @param trainingUUID UUID of training.
     * @return local entries for keys corresponding to {@code featureIndexes}.
     */
    public static Iterable<Cache.Entry<SplitKey, IgniteBiTuple<Integer, Double>>> localEntries(
        Set<Integer> featureIndexes,
        IgniteBiFunction<Integer, Ignite, Object> affinity,
        UUID trainingUUID) {
        Ignite ignite = Ignition.localIgnite();
        Set<SplitKey> keys = featureIndexes.stream().map(fIdx -> new SplitKey(trainingUUID, affinity.apply(fIdx, ignite), fIdx)).collect(Collectors.toSet());

        Collection<SplitKey> locKeys = affinity().mapKeysToNodes(keys).getOrDefault(ignite.cluster().localNode(), Collections.emptyList());

        return () -> {
            Function<SplitKey, Cache.Entry<SplitKey, IgniteBiTuple<Integer, Double>>> f = k -> (new CacheEntryImpl<>(k, getOrCreate(ignite).localPeek(k)));
            return locKeys.stream().map(f).iterator();
        };
    }

    /**
     * Clears data related to current training from splits cache related to given training.
     *
     * @param featuresCnt Count of features.
     * @param affinity Affinity function.
     * @param uuid UUID of the given training.
     * @param ignite Ignite instance.
     */
    public static void clear(int featuresCnt, IgniteBiFunction<Integer, Ignite, Object> affinity, UUID uuid,
        Ignite ignite) {
        Set<SplitKey> toRmv = IntStream.range(0, featuresCnt).boxed().map(fIdx -> new SplitKey(uuid, affinity.apply(fIdx, ignite), fIdx)).collect(Collectors.toSet());

        getOrCreate(ignite).removeAll(toRmv);
    }
}
