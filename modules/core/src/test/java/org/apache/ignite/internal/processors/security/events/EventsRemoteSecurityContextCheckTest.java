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

package org.apache.ignite.internal.processors.security.events;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.lang.IgnitePredicate;
import org.junit.Test;

/**
 * Testing operation security context when the remote filter of IgniteEvents is executed on remote nodes.
 * <p>
 * The initiator node broadcasts a task to 'run' nodes that register the remote filter on check nodes. The filter is
 * executed on 'check' nodes. On every step, it is performed verification that operation security context is the
 * initiator context.
 */
public class EventsRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** Server node to change cache state. */
    private static final String SRV = "srv";

    /** Name of server feature transit node. */
    private static final String SRV_CHECK_ADDITIONAL = "srv_check_additional";

    /** Index to generate a unique topic and the synchronized set value. */
    private static final AtomicInteger INDEX = new AtomicInteger();

    /** Map to sync filters. */
    private static final Map<String, CountDownLatch> SYNC_MAP = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV);

        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startClientAllowAll(CLNT_RUN);

        startGridAllowAll(SRV_CHECK);

        startGridAllowAll(SRV_CHECK_ADDITIONAL);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        return super.getConfiguration(instanceName).setIncludeEventTypes(EventType.EVTS_ALL);
    }

    /** */
    @Test
    public void testRemoteListen() {
        BiFunction<IgniteEvents, String, UUID> f = new BiFunction<IgniteEvents, String, UUID>() {
            @Override public UUID apply(IgniteEvents evts, String cacheName) {
                return evts.remoteListen((uuid, e) -> true, remoteFilter(cacheName), EventType.EVT_CACHE_OBJECT_PUT);
            }
        };

        execute(f);
    }

    /** */
    @Test
    public void testRemoteListenAsync() {
        BiFunction<IgniteEvents, String, UUID> f = new BiFunction<IgniteEvents, String, UUID>() {
            @Override public UUID apply(IgniteEvents evts, String cacheName) {
                return evts.remoteListenAsync((uuid, e) -> true, remoteFilter(cacheName), EventType.EVT_CACHE_OBJECT_PUT)
                    .get();
            }
        };

        execute(f);
    }

    /** */
    private void execute(BiFunction<IgniteEvents, String, UUID> func) {
        SYNC_MAP.clear();

        runAndCheck(() -> {
            String cacheName = "test_cache_" + INDEX.incrementAndGet();

            SYNC_MAP.put(cacheName, new CountDownLatch(nodesToCheck().size()));

            IgniteCache<String, String> cache = grid(SRV).createCache(new CacheConfiguration<String, String>(cacheName)
                .setCacheMode(CacheMode.REPLICATED));

            Ignite loc = Ignition.localIgnite();

            IgniteEvents evts = loc.events(loc.cluster().forNodeIds(nodesToCheckIds()));

            UUID id = func.apply(evts, cacheName);

            try {
                cache.put("key", "value");

                SYNC_MAP.get(cacheName).await(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            finally {
                evts.stopRemoteListen(id);
            }
        });
    }

    /** */
    private IgnitePredicate<Event> remoteFilter(final String cacheName) {
        return e -> {
            CacheEvent evt = (CacheEvent)e;

            if (cacheName.equals(evt.cacheName())) {
                VERIFIER.register(OPERATION_CHECK);

                SYNC_MAP.get(cacheName).countDown();
            }

            return true;
        };
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> endpoints() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> nodesToCheck() {
        return Arrays.asList(SRV_CHECK, SRV_CHECK_ADDITIONAL);
    }
}
