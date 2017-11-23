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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer;
import org.apache.ignite.ml.trees.trainers.columnbased.RegionProjection;

/**
 * Cache used for storing data of region projections on features.
 */
public class ProjectionsCache {
    /**
     * Name of cache which is used for storing data of region projections on features of {@link
     * ColumnDecisionTreeTrainer}.
     */
    public static final String CACHE_NAME = "COLUMN_DECISION_TREE_TRAINER_PROJECTIONS_CACHE_NAME";

    /**
     * Key of region projections cache.
     */
    public static class RegionKey {
        /** Column key of cache used as input for {@link ColumnDecisionTreeTrainer}. */
        @AffinityKeyMapped
        private final Object parentColKey;

        /** Feature index. */
        private final int featureIdx;

        /** Region index. */
        private final int regBlockIdx;

        /** Training UUID. */
        private final UUID trainingUUID;

        /**
         * Construct a RegionKey from feature index, index of block, key of column in input cache and UUID of training.
         *
         * @param featureIdx Feature index.
         * @param regBlockIdx Index of block.
         * @param parentColKey Key of column in input cache.
         * @param trainingUUID UUID of training.
         */
        public RegionKey(int featureIdx, int regBlockIdx, Object parentColKey, UUID trainingUUID) {
            this.featureIdx = featureIdx;
            this.regBlockIdx = regBlockIdx;
            this.trainingUUID = trainingUUID;
            this.parentColKey = parentColKey;
        }

        /**
         * Feature index.
         *
         * @return Feature index.
         */
        public int featureIdx() {
            return featureIdx;
        }

        /**
         * Region block index.
         *
         * @return Region block index.
         */
        public int regionBlockIndex() {
            return regBlockIdx;
        }

        /**
         * UUID of training.
         *
         * @return UUID of training.
         */
        public UUID trainingUUID() {
            return trainingUUID;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            RegionKey key = (RegionKey)o;

            if (featureIdx != key.featureIdx)
                return false;
            if (regBlockIdx != key.regBlockIdx)
                return false;
            return trainingUUID != null ? trainingUUID.equals(key.trainingUUID) : key.trainingUUID == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = trainingUUID != null ? trainingUUID.hashCode() : 0;
            res = 31 * res + featureIdx;
            res = 31 * res + regBlockIdx;
            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "RegionKey [" +
                "parentColKey=" + parentColKey +
                ", featureIdx=" + featureIdx +
                ", regBlockIdx=" + regBlockIdx +
                ", trainingUUID=" + trainingUUID +
                ']';
        }
    }

    /**
     * Affinity service for region projections cache.
     *
     * @return Affinity service for region projections cache.
     */
    public static Affinity<RegionKey> affinity() {
        return Ignition.localIgnite().affinity(CACHE_NAME);
    }

    /**
     * Get or create region projections cache.
     *
     * @param ignite Ignite instance.
     * @return Region projections cache.
     */
    public static IgniteCache<RegionKey, List<RegionProjection>> getOrCreate(Ignite ignite) {
        CacheConfiguration<RegionKey, List<RegionProjection>> cfg = new CacheConfiguration<>();

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
     * Get region projections in the form of map (regionIndex -> regionProjections).
     *
     * @param featureIdx Feature index.
     * @param maxDepth Max depth of decision tree.
     * @param regionIndexes Indexes of regions for which we want get projections.
     * @param blockSize Size of regions block.
     * @param affinity Affinity function.
     * @param trainingUUID UUID of training.
     * @param ignite Ignite instance.
     * @return Region projections in the form of map (regionIndex -> regionProjections).
     */
    public static Map<Integer, RegionProjection> projectionsOfRegions(int featureIdx, int maxDepth,
        IntStream regionIndexes, int blockSize, IgniteFunction<Integer, Object> affinity, UUID trainingUUID,
        Ignite ignite) {
        HashMap<Integer, RegionProjection> regsForSearch = new HashMap<>();
        IgniteCache<RegionKey, List<RegionProjection>> cache = getOrCreate(ignite);

        PrimitiveIterator.OfInt itr = regionIndexes.iterator();

        int curBlockIdx = -1;
        List<RegionProjection> block = null;

        Object affinityKey = affinity.apply(featureIdx);

        while (itr.hasNext()) {
            int i = itr.nextInt();

            int blockIdx = i / blockSize;

            if (blockIdx != curBlockIdx) {
                block = cache.localPeek(key(featureIdx, blockIdx, affinityKey, trainingUUID));
                curBlockIdx = blockIdx;
            }

            if (block == null)
                throw new IllegalStateException("Unexpected null block at index " + i);

            RegionProjection reg = block.get(i % blockSize);

            if (reg.depth() < maxDepth)
                regsForSearch.put(i, reg);
        }

        return regsForSearch;
    }

    /**
     * Returns projections of regions on given feature filtered by maximal depth in the form of (region index -> region
     * projection).
     *
     * @param featureIdx Feature index.
     * @param maxDepth Maximal depth of the tree.
     * @param regsCnt Count of regions.
     * @param blockSize Size of regions blocks.
     * @param affinity Affinity function.
     * @param trainingUUID UUID of training.
     * @param ignite Ignite instance.
     * @return Projections of regions on given feature filtered by maximal depth in the form of (region index -> region
     * projection).
     */
    public static Map<Integer, RegionProjection> projectionsOfFeature(int featureIdx, int maxDepth, int regsCnt,
        int blockSize, IgniteFunction<Integer, Object> affinity, UUID trainingUUID, Ignite ignite) {
        return projectionsOfRegions(featureIdx, maxDepth, IntStream.range(0, regsCnt), blockSize, affinity, trainingUUID, ignite);
    }

    /**
     * Construct key for projections cache.
     *
     * @param featureIdx Feature index.
     * @param regBlockIdx Region block index.
     * @param parentColKey Column key of cache used as input for {@link ColumnDecisionTreeTrainer}.
     * @param uuid UUID of training.
     * @return Key for projections cache.
     */
    public static RegionKey key(int featureIdx, int regBlockIdx, Object parentColKey, UUID uuid) {
        return new RegionKey(featureIdx, regBlockIdx, parentColKey, uuid);
    }

    /**
     * Clear data from projections cache related to given training.
     *
     * @param featuresCnt Features count.
     * @param regs Regions count.
     * @param aff Affinity function.
     * @param uuid UUID of training.
     * @param ignite Ignite instance.
     */
    public static void clear(int featuresCnt, int regs, IgniteBiFunction<Integer, Ignite, Object> aff, UUID uuid,
        Ignite ignite) {
        Set<RegionKey> toRmv = IntStream.range(0, featuresCnt).boxed().
            flatMap(fIdx -> IntStream.range(0, regs).boxed().map(reg -> new IgniteBiTuple<>(fIdx, reg))).
            map(t -> key(t.get1(), t.get2(), aff.apply(t.get1(), ignite), uuid)).
            collect(Collectors.toSet());

        getOrCreate(ignite).removeAll(toRmv);
    }
}
