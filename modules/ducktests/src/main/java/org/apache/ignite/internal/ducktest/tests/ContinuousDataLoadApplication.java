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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.transactions.Transaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Keeps data load until stopped.
 */
public class ContinuousDataLoadApplication extends AbstractDataLoadApplication {
    /** Logger. */
    private static final Logger log = LogManager.getLogger(ContinuousDataLoadApplication.class);

    /** Load config. */
    private Config cfg;

    /** Node set to exclusively put data on if required. */
    private List<ClusterNode> nodesToLoad = Collections.emptyList();

    /** */
    private Affinity<Integer> aff;

    /** {@inheritDoc} */
    @Override protected void loadData() {
        log.info("Generating data in background...");

        if (cfg.targetNodes != null && !cfg.targetNodes.isEmpty()) {
            nodesToLoad = ignite.cluster().nodes().stream().filter(n -> cfg.targetNodes.contains(n.id().toString()))
                .collect(Collectors.toList());

            aff = ignite.affinity(cacheName(1));
        }

        int warmUpCnt = cfg.warmUpRange < 1 ? (int)Math.max(1, 0.1f * cfg.range) : cfg.warmUpRange;

        IgniteCache<Integer, BinaryObject> cache = ignite.cache(cacheName(1));

        long notifyTime = System.nanoTime();

        int loaded = 0;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (active()) {
            try (Transaction tx = cfg.transactional ? ignite.transactions().txStart() : null) {
                for (int i = 0; i < cfg.range && active(); ++i) {
                    int key = rnd.nextInt();

                    if (skipDataKey(key))
                        continue;

                    cache.put(key, cacheEntryValue(key));

                    ++loaded;

                    if (notifyTime + TimeUnit.MILLISECONDS.toNanos(1500) < System.nanoTime()) {
                        log.info("Put " + loaded + " entries into " + cache.getName());

                        notifyTime = System.nanoTime();
                    }

                    if (warmUpCnt == loaded)
                        log.info("Warm up finished.");
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
    @Override protected void parseConfig(JsonNode json) {
        super.parseConfig(json);

        cfg = parseConfig(json, Config.class);
    }

    /**
     * The configuration holder.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Config {
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
