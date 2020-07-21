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

package org.apache.ignite.internal.processors.security.sandbox;

import java.security.AccessControlException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Checks that a remote filter for IgniteEvents is executed inside the sandbox.
 */
public class EventsSandboxTest extends AbstractSandboxTest {
    /** Server node to change cache state. */
    private static final String SRV = "srv";

    /** Index to generate a unique topic and the synchronized set value. */
    private static final AtomicInteger INDEX = new AtomicInteger();

    /** Latch. */
    private static volatile CountDownLatch latch;

    /** Error. */
    private static volatile AccessControlException error;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        return super.getConfiguration(instanceName).setIncludeEventTypes(EventType.EVT_CACHE_OBJECT_PUT);
    }

    /** */
    @Test
    public void testRemoteFilter() {
        testEvents(new BiFunction<IgniteEvents, String, UUID>() {
            @Override public UUID apply(IgniteEvents evts, String cacheName) {
                return evts.remoteListen((uuid, e) -> true, remoteFilter(cacheName), EventType.EVT_CACHE_OBJECT_PUT);
            }
        });
    }

    /** */
    @Test
    public void testRemoteFilterAsync() {
        testEvents(new BiFunction<IgniteEvents, String, UUID>() {
            @Override public UUID apply(IgniteEvents evts, String cacheName) {
                return evts.remoteListenAsync((uuid, e) -> true, remoteFilter(cacheName), EventType.EVT_CACHE_OBJECT_PUT)
                    .get();
            }
        });
    }

    /** */
    private void testEvents(BiFunction<IgniteEvents, String, UUID> f) {
        execute(grid(CLNT_ALLOWED_WRITE_PROP), f, false);
        execute(grid(CLNT_FORBIDDEN_WRITE_PROP), f, true);
    }

    /** */
    private void execute(Ignite node, BiFunction<IgniteEvents, String, UUID> func, boolean isForbiddenCase) {
        final String cacheName = "test_cache_" + INDEX.getAndIncrement();

        grid(SRV).createCache(new CacheConfiguration<>(cacheName).setCacheMode(CacheMode.REPLICATED));

        IgniteEvents evts = node.events(node.cluster().forNodeId(grid(SRV).localNode().id()));

        UUID id = func.apply(evts, cacheName);

        try {
            GridTestUtils.RunnableX r = () -> {
                error = null;

                latch = new CountDownLatch(1);

                grid(SRV).cache(cacheName).put("key", "value");

                latch.await(10, TimeUnit.SECONDS);

                if (error != null)
                    throw error;
            };

            if (isForbiddenCase)
                runForbiddenOperation(r, AccessControlException.class);
            else
                runOperation(r);

            assertEquals("value", grid(SRV).cache(cacheName).get("key"));
        }
        finally {
            evts.stopRemoteListen(id);
        }
    }

    /** */
    private IgnitePredicate<Event> remoteFilter(final String cacheName) {
        return new IgnitePredicate<Event>() {
            @IgniteInstanceResource
            private Ignite ign;

            @LoggerResource
            private IgniteLogger log;

            @Override public boolean apply(Event evt) {
                assertNotNull("Ignite Instance Resource shouldn't be null.", ign);
                assertNotNull("Ignite Logger shouldn't be null.", log);

                CacheEvent cevt = (CacheEvent)evt;

                if (cacheName.equals(cevt.cacheName())) {
                    try {
                        controlAction();
                    }
                    catch (AccessControlException e) {
                        error = e;
                    }
                    finally {
                        latch.countDown();
                    }
                }

                return true;
            }
        };
    }
}
