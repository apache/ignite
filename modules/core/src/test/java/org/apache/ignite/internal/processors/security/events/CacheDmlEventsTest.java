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

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.junit.Test;

/**
 * Test that an event's local listener and an event's remote filter get correct subjectId when a cache performs an
 * operation on data.
 */
public class CacheDmlEventsTest extends AbstractSecurityTest {
    /** Counter. */
    private static final AtomicInteger COUNTER = new AtomicInteger();

    /** Node that registers event listeners. */
    private static final String LISTENER_NODE = "listener_node";

    /** Client node. */
    static final String CLNT = "client";

    /** Server node. */
    static final String SRV = "server";

    /** Events latch. */
    private static CountDownLatch evtsLatch;
    /**
     *
     */
    private static final AtomicInteger rmtLoginCnt = new AtomicInteger();
    /**
     *
     */
    private static final AtomicInteger locLoginCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll(LISTENER_NODE);
        startGridAllowAll(SRV);
        startClientAllowAll(CLNT);
    }

    @Test
    public void testPutEvent() throws Exception {
        Consumer<IgniteCache> op = c -> c.put("key", "value");

        testOperation(CLNT, EventType.EVT_CACHE_OBJECT_PUT, op);
        testOperation(SRV, EventType.EVT_CACHE_OBJECT_PUT, op);
    }

    @Test
    public void testReadEvent() throws Exception {
        Consumer<IgniteCache> op = c -> {
            c.put("key", "VALUE");

            c.get("key");
        };

        testOperation(CLNT, EventType.EVT_CACHE_OBJECT_READ, op);
        testOperation(SRV, EventType.EVT_CACHE_OBJECT_READ, op);
    }

    @Test
    public void testRemoveEvent() throws Exception {
        Consumer<IgniteCache> op = c -> {
            c.put("key", "VALUE");

            c.remove("key");
        };

        testOperation(CLNT, EventType.EVT_CACHE_OBJECT_REMOVED, op);
        testOperation(SRV, EventType.EVT_CACHE_OBJECT_REMOVED, op);
    }

    private void testOperation(String expLogin, int evtType, Consumer<IgniteCache> op) throws Exception {
        final String cacheName = "test_cache_" + COUNTER.incrementAndGet();

        grid(LISTENER_NODE).createCache(new CacheConfiguration<>(cacheName)
//            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

        int expTimes = 1;
        evtsLatch = new CountDownLatch(2);

        rmtLoginCnt.set(0);
        locLoginCnt.set(0);

        UUID lsnrId = grid(LISTENER_NODE).events().remoteListen(
            new IgniteBiPredicate<UUID, Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(UUID uuid, Event evt) {
                    onEvent(ign, "local", (CacheEvent)evt, cacheName, expLogin);
                    return true;
                }
            },
            new IgnitePredicate<Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(Event evt) {
                    new Exception("MY_DEBUG inside remote listener").printStackTrace();
                    onEvent(ign, "remote", (CacheEvent)evt, cacheName, expLogin);
                    return true;
                }
            }, evtType);

        try {
            op.accept(grid(expLogin).cache(cacheName));
            // Waiting for events.
            evtsLatch.await(10, TimeUnit.SECONDS);

            assertEquals("Remote filter.", expTimes, rmtLoginCnt.get());
            assertEquals("Local listener.", expTimes, locLoginCnt.get());
        }
        finally {
            grid(LISTENER_NODE).events().stopRemoteListen(lsnrId);
        }
    }

    /**
     *
     */
    private static boolean onEvent(IgniteEx ign, String prefix, CacheEvent evt, String cacheName, String expLogin) {
        if (!cacheName.equals(evt.cacheName()))
            return false;

        evtsLatch.countDown();

        if ("remote".equals(prefix))
            rmtLoginCnt.incrementAndGet();
        else
            locLoginCnt.incrementAndGet();

        try {
//            IgniteEx ign = IgnitionEx.localIgnite();
            SecuritySubject subj = ign.context().security().authenticatedSubject(evt.subjectId());

            String login = subj.login().toString();

            System.out.println(
                "MY_DEBUG " + prefix + " loc=" + ign.context().igniteInstanceName() +
                    ", evtLogin=" + login + ", expLogin=" + expLogin
            );

            assertEquals(expLogin, login);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setIncludeEventTypes(EventType.EVTS_CACHE);
    }

}
