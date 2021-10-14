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

package org.apache.ignite.internal.ducktest.tests.control_utility;

import java.util.concurrent.ThreadLocalRandom;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class InconsistentNodeApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        String cacheName = jsonNode.get("cacheName").asText();
        int amount = jsonNode.get("amount").asInt();
        int parts = jsonNode.get("parts").asInt();
        boolean tx = jsonNode.get("tx").asBoolean();

        markInitialized();

        waitForActivation();

        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(cacheName);

        cfg.setAtomicityMode(tx ? TRANSACTIONAL : ATOMIC);
        cfg.setCacheMode(CacheMode.REPLICATED);
        cfg.setAffinity(new RendezvousAffinityFunction().setPartitions(parts));

        ignite.getOrCreateCache(cfg);

        GridCacheVersionManager mgr =
            ((GridCacheAdapter)((IgniteEx)ignite).cachex(cacheName).cache()).context().shared().versions();

        int cnt = 0;

        for (int key = 0; key < amount; key += ThreadLocalRandom.current().nextInt(1, 3)) { // Random shift.
            IgniteInternalCache<?, ?> cache = ((IgniteEx)ignite).cachex(cacheName);

            GridCacheAdapter<?, ?> adapter = (GridCacheAdapter)cache.cache();

            GridCacheEntryEx entry = adapter.entryEx(key);

            boolean init = entry.initialValue(
                new CacheObjectImpl(cnt, null), // Incremental value.
                mgr.next(entry.context().kernalContext().discovery().topologyVersion()), // Incremental version.
                0,
                0,
                false,
                AffinityTopologyVersion.NONE,
                GridDrType.DR_NONE,
                false,
                false);

            assert init : "iterableKey " + key + " already inited";

            if (cnt % 1_000 == 0)
                log.info("APPLICATION_STREAMED [entries=" + cnt + "]");

            cnt++;
        }

        log.info("APPLICATION_STREAMING_FINISHED [entries=" + cnt + "]");

        while (!terminated())
            U.sleep(100); // Keeping node alive.

        markFinished();
    }
}
