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

package org.apache.ignite.internal.ducktest.tests.smoke_test;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Checks if clocks are synchronized between nodes.
 */
public class ClockSyncCheckerApplication extends IgniteAwareApplication {
    /** */
    private static final String TIMESTAMP_CACHE = "TimestampCache";

    /** */
    private static final String MAX_CLOCK_DIVERGENCY_MS_PARAM = "maxClockDivergencyMs";

    /** */
    private static final long WAIT_TIMEOUT_MS = 5000;

    /**
     * Client node sends requests to all servers by putting distributed cache entries.
     * Receiver gets request through remote event filter and replies by writing with its local timestamp to cache.
     * Client listens for replies with continuous query and checks the difference between clocks.
     * <p>
     * Cache event handler does not allow to modify entries being handled (even asynchronously),
     * so request and reply have different keys, structured as 2-tuples:
     * request = (targetNodeId, 0), reply = (targetNodeId, timestamp); values are not used.
     * <p>
     * Cache mode should be replicated in order to run remote listeners on all server nodes reliably.
     */
    @Override public void run(JsonNode jsonNode) throws Exception {
        long maxClockDivergencyMs = jsonNode.get(MAX_CLOCK_DIVERGENCY_MS_PARAM).asLong();

        IgniteCache<T2<UUID, Long>, Optional<Void>> cache = ignite.getOrCreateCache(
            new CacheConfiguration<T2<UUID, Long>, Optional<Void>>(TIMESTAMP_CACHE)
                .setCacheMode(CacheMode.REPLICATED));

        cache.clear();

        markInitialized();

        ignite.cluster().forServers().nodes().forEach(server ->
            ignite.events(ignite.cluster().forNode(server)).remoteListen(
                null,
                evt -> {
                    T2<UUID, Long> evtKey = ((CacheEvent)evt).key();

                    if (server.id().equals(evtKey.get1()) && evtKey.get2() == 0L) {
                        long ts = System.currentTimeMillis();

                        cache.putAsync(new T2<>(server.id(), ts), Optional.empty());
                    }

                    return true;
                },
                EVT_CACHE_OBJECT_PUT));

        Map<UUID, Long> requestTimestamps = new ConcurrentHashMap<>();

        CountDownLatch replies = new CountDownLatch(ignite.cluster().nodes().size() - 1);
        Collection<UUID> nodesReplied = Collections.newSetFromMap(new ConcurrentHashMap<>());

        AtomicBoolean broken = new AtomicBoolean();

        ContinuousQuery<T2<UUID, Long>, Optional<Void>> qry = new ContinuousQuery<>();

        qry.setLocalListener(evts -> evts.forEach(evt -> {
            long rmtTs = evt.getKey().get2();

            if (rmtTs != 0) {
                UUID serverId = evt.getKey().get1();

                if (nodesReplied.add(serverId)) {
                    long expectedTs = (System.currentTimeMillis() + requestTimestamps.get(serverId)) / 2;
                    long diff = expectedTs - rmtTs;

                    String msg = String.format("Clock delta between real and expected values for server %s is %d ms.",
                        serverId, diff);

                    log.info(msg);

                    if (Math.abs(diff) > maxClockDivergencyMs && !broken.get()) {
                        broken.set(true);

                        markBroken(new RuntimeException(msg));
                    }

                    replies.countDown();
                }
            }
        }));

        try (QueryCursor<Cache.Entry<T2<UUID, Long>, Optional<Void>>> ignored = cache.query(qry)) {
            for (ClusterNode node : ignite.cluster().forRemotes().nodes()) {
                requestTimestamps.put(node.id(), System.currentTimeMillis());

                cache.put(new T2<>(node.id(), 0L), Optional.empty());
            }

            assert replies.await(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }

        markFinished();
    }
}
