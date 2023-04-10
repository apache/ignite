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

package org.apache.ignite.internal.ducktest.tests;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.transactions.Transaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Keeps data load until stopped.
 */
public class ContinuousDataLoadApplication extends DataGenerationApplication {
    /** Logger. */
    private static final Logger log = LogManager.getLogger(ContinuousDataLoadApplication.class);

    /** Node set to exclusively put data on if required. */
    private List<ClusterNode> nodesToLoad = Collections.emptyList();

    /** */
    private Affinity<Integer> aff;

    /** Data number to put before notifying of the initialized state. */
    private int warmUpCnt;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        Config cfg = parseConfig(jsonNode);

        init(cfg);

        log.info("Generating data in background...");

        long notifyTime = System.nanoTime();

        int loaded = 0;

        while (active()) {
            try (Transaction tx = cfg.transactional ? ignite.transactions().txStart() : null) {
                for (int i = 0; i < cfg.range && active(); ++i) {
                    if (skipDataKey(i))
                        continue;

                    cache.put(i, i);

                    ++loaded;

                    if (notifyTime + TimeUnit.MILLISECONDS.toNanos(1500) < System.nanoTime())
                        notifyTime = System.nanoTime();

                    // Delayed notify of the initialization to make sure the data load has completelly began and
                    // has produced some valuable amount of data.
                    if (!inited() && warmUpCnt == loaded)
                        markInitialized();
                }

                if (tx != null && active())
                    tx.commit();
            }
        }

        log.info("Background data generation finished.");

        markFinished();
    }

    /**
     * @return {@code True} if data should not be put for {@code dataKey}. {@code False} otherwise.
     */
    private boolean skipDataKey(int dataKey) {
        if (!nodesToLoad.isEmpty()) {
            for (ClusterNode n : nodesToLoad) {
                if (aff.isPrimary(n, dataKey))
                    return false;
            }

            return true;
        }

        return false;
    }

    /**
     * Prepares run settings based on {@code cfg}.
     */
    @Override protected void createCaches(int cacheCnt, boolean transactional, int backups, int idxCnt) {
        super.createCaches(cacheCnt, transactional, backups, idxCnt);

        if (cfg.targetNodes != null && !cfg.targetNodes.isEmpty()) {
            nodesToLoad = ignite.cluster().nodes().stream().filter(n -> cfg.targetNodes.contains(n.id().toString()))
                .collect(Collectors.toList());

            aff = ignite.affinity(cfg.cacheName);
        }

        warmUpCnt = cfg.warmUpRange < 1 ? (int)Math.max(1, 0.1f * cfg.range) : cfg.warmUpRange;
    }

    /**
     * Converts Json-represented config into {@code Config}.
     */
    private static Config parseConfig(JsonNode node) {
        ObjectMapper objMapper = new ObjectMapper();
        objMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        Config cfg;

        try {
            cfg = objMapper.treeToValue(node, Config.class);
        }
        catch (Exception e) {
            throw new IllegalStateException("Unable to parse config.", e);
        }

        return cfg;
    }

    /**
     * The configuration holder.
     */
    private static class Config {
        /** Name of the cache. */
        private String cacheName;

        /** Data/keys number to load. */
        private int range;

        /** Node id set. If not empty, data will be load only on this nodes. */
        private Set<String> targetNodes;

        /** If {@code true}, data will be put within transaction. */
        private boolean transactional;

        /**
         * Data number to warn-up and to delay the init-notification. If < 1, ignored and considered default 10% of
         * {@code range}.
         */
        private int warmUpRange;
    }
}
