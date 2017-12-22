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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.trees.ContinuousRegionInfo;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer;
import org.apache.ignite.ml.trees.trainers.columnbased.TrainingContext;

/**
 * Class for operations related to cache containing training context for {@link ColumnDecisionTreeTrainer}.
 */
public class ContextCache {
    /**
     * Name of cache containing training context for {@link ColumnDecisionTreeTrainer}.
     */
    public static final String COLUMN_DECISION_TREE_TRAINER_CONTEXT_CACHE_NAME = "COLUMN_DECISION_TREE_TRAINER_CONTEXT_CACHE_NAME";

    /**
     * Get or create cache for training context.
     *
     * @param ignite Ignite instance.
     * @param <D> Class storing information about continuous regions.
     * @return Cache for training context.
     */
    public static <D extends ContinuousRegionInfo> IgniteCache<UUID, TrainingContext<D>> getOrCreate(Ignite ignite) {
        CacheConfiguration<UUID, TrainingContext<D>> cfg = new CacheConfiguration<>();

        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cfg.setEvictionPolicy(null);

        cfg.setCopyOnRead(false);

        cfg.setCacheMode(CacheMode.REPLICATED);

        cfg.setOnheapCacheEnabled(true);

        cfg.setReadFromBackup(true);

        cfg.setName(COLUMN_DECISION_TREE_TRAINER_CONTEXT_CACHE_NAME);

        return ignite.getOrCreateCache(cfg);
    }
}
