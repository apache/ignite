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

import java.util.Random;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Keeps data load until stopped.
 */
public class ContinuousDataLoadApplication extends IgniteAwareApplication {
    /** Logger. */
    private static final Logger log = LogManager.getLogger(ContinuousDataLoadApplication.class.getName());

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        String cacheName = jsonNode.get("cacheName").asText();
        int range = jsonNode.get("range").asInt();

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheName);

        int warmUpCnt = (int)Math.max(1, 0.1f * range);

        Random rnd = new Random();

        long streamed = 0;

        log.info("Generating data in background...");

        long notifyTime = System.nanoTime();

        while (active()) {
            cache.put(rnd.nextInt(range), rnd.nextInt(range));

            streamed++;

            if (notifyTime + U.millisToNanos(1500) < System.nanoTime()) {
                notifyTime = System.nanoTime();

                if (log.isDebugEnabled())
                    log.debug("Streamed " + streamed + " entries.");
            }

            // Delayed notify of the initialization to make sure the data load has completelly began and
            // has produced some valuable amount of data.
            if (!inited() && warmUpCnt == streamed)
                markInitialized();
        }

        log.info("Background data generation finished.");

        markFinished();
    }
}
