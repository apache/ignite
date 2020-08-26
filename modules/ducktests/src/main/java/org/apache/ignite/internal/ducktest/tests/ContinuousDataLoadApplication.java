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

import java.util.Set;
import java.io.IOException;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Keeps data load until stopped.
 */
public class ContinuousDataLoadApplication extends IgniteAwareApplication {
    /** Logger. */
    private static final Logger log = LogManager.getLogger(ContinuousDataLoadApplication.class.getName());

    /**
     * Configuration holder.
     */
    private static class Config {
        /** */
        private String cacheName;

        /** */
        private int range;

        /** */
        private Set<String> targetNodes;

        /** */
        private boolean transactional;
    }

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        Config cfg = parseConfig(jsonNode);

        CacheConfiguration<Integer, Integer> cacheConfiguration = new CacheConfiguration<>(cfg.cacheName);

        cacheConfiguration.setAtomicityMode(cfg.transactional ? TRANSACTIONAL : ATOMIC);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheConfiguration);

        int warmUpCnt = (int)Math.max(1, 0.1f * cfg.range);

        log.info("Generating data in background...");

        long notifyTime = System.nanoTime();

        while (active()) {
            Transaction tx = cfg.transactional ? ignite.transactions().txStart() : null;

            //TODO
            if (tx != null)
                log.warn("TODO: transaction started.");

            for (int i = 0; i < cfg.range; ++i) {
                cache.put(i, i);

                if (notifyTime + U.millisToNanos(1500) < System.nanoTime()) {
                    notifyTime = System.nanoTime();

                    if (log.isDebugEnabled())
                        log.debug("Streamed " + i + " entries.");
                }

                // Delayed notify of the initialization to make sure the data load has completelly began and
                // has produced some valuable amount of data.
                if (!inited() && warmUpCnt == i)
                    markInitialized();
            }

            if (tx != null) {
                //TODO
                log.warn("TODO: commiting.");

                try {
                    tx.commit();
                } finally {
                    tx.close();
                }

                //TODO
                log.warn("Commited.");
            }
        }

        log.info("Background data generation finished.");

        markFinished();
    }

    /** */
    private static Config parseConfig(JsonNode node) {
        ObjectMapper objMapper = new ObjectMapper();
        objMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        Config cfg;
   
        //TODO
        log.warn("TODO : Parsing config: " + node.toString());

        try {
            cfg = objMapper.treeToValue(node, Config.class);
        }
        catch (Exception e) {
            throw new IllegalStateException("Unable to parse config.", e);
        }

        return cfg;
    }


    /** */
    public static void main(String[] args){
        ObjectMapper objMapper = new ObjectMapper();
        objMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        String json = "{\"cacheName\":\"test-cache\",\"range\":100000,\"transactional\":true}";

        Config cfg;

        try {
            cfg = objMapper.readValue(json, Config.class);
            System.out.println("Cfg: " +  cfg);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
