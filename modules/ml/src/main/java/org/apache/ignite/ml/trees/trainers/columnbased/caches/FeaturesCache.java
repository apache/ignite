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

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer;

/**
 * Cache storing features for {@link ColumnDecisionTreeTrainer}.
 */
public class FeaturesCache {
    /**
     * Name of cache which is used for storing features for {@link ColumnDecisionTreeTrainer}.
     */
    public static final String COLUMN_DECISION_TREE_TRAINER_FEATURES_CACHE_NAME = "COLUMN_DECISION_TREE_TRAINER_FEATURES_CACHE_NAME";

    /**
     * Key of features cache.
     */
    public static class FeatureKey {
        /** Column key of cache used as input for {@link ColumnDecisionTreeTrainer}. */
        @AffinityKeyMapped
        private Object parentColKey;

        /** Index of feature. */
        private int featureIdx;

        /** UUID of training. */
        private UUID trainingUUID;

        /**
         * Construct FeatureKey.
         *
         * @param featureIdx Feature index.
         * @param trainingUUID UUID of training.
         * @param parentColKey Column key of cache used as input.
         */
        public FeatureKey(int featureIdx, UUID trainingUUID, Object parentColKey) {
            this.parentColKey = parentColKey;
            this.featureIdx = featureIdx;
            this.trainingUUID = trainingUUID;
            this.parentColKey = parentColKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            FeatureKey key = (FeatureKey)o;

            if (featureIdx != key.featureIdx)
                return false;
            return trainingUUID != null ? trainingUUID.equals(key.trainingUUID) : key.trainingUUID == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = trainingUUID != null ? trainingUUID.hashCode() : 0;
            res = 31 * res + featureIdx;
            return res;
        }
    }

    /**
     * Create new projections cache for ColumnDecisionTreeTrainer if needed.
     *
     * @param ignite
     */
    public static IgniteCache<FeatureKey, double[]> getOrCreate(Ignite ignite) {
        CacheConfiguration<FeatureKey, double[]> cfg = new CacheConfiguration<>();

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

        cfg.setOnheapCacheEnabled(true);

        cfg.setBackups(0);

        cfg.setName(COLUMN_DECISION_TREE_TRAINER_FEATURES_CACHE_NAME);

        return ignite.getOrCreateCache(cfg);
    }

    /**
     * Construct FeatureKey from index, uuid and affinity key.
     *
     * @param idx Feature index.
     * @param uuid UUID of training.
     * @param aff Affinity key.
     * @return FeatureKey.
     */
    public static FeatureKey getFeatureCacheKey(int idx, UUID uuid, Object aff) {
        return new FeatureKey(idx, uuid, aff);
    }

    /**
     * Clear all data from features cache related to given training.
     *
     * @param featuresCnt Count of features.
     * @param affinity Affinity function.
     * @param uuid Training uuid.
     * @param ignite Ignite instance.
     */
    public static void clear(int featuresCnt, IgniteBiFunction<Integer, Ignite, Object> affinity, UUID uuid,
        Ignite ignite) {
        Set<FeatureKey> toRmv = IntStream.range(0, featuresCnt).boxed().map(fIdx -> getFeatureCacheKey(fIdx, uuid, affinity.apply(fIdx, ignite))).collect(Collectors.toSet());

        getOrCreate(ignite).removeAll(toRmv);
    }
}
