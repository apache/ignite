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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestProcessor;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;

/**
 * Test that an event's local listener and an event's remote filter get correct subjectId when a cache performs an
 * operation on data.
 */
@RunWith(Parameterized.class)
public class CacheEventsTest extends AbstractSecurityTest {
    /** Counter to name caches. */
    private static final AtomicInteger COUNTER = new AtomicInteger();

    /** Node that registers event listeners. */
    private static final String LISTENER_NODE = "listener_node";

    /** Client node. */
    static final String CLNT = "client";

    /** Server node. */
    static final String SRV = "server";

    /** Events latch. */
    private static CountDownLatch evtsLatch;

    /** */
    private static final AtomicInteger rmtLoginCnt = new AtomicInteger();

    /** */
    private static final AtomicInteger locLoginCnt = new AtomicInteger();

    /** */
    @Parameterized.Parameter()
    public CacheAtomicityMode atomicMode;

    /** */
    @Parameterized.Parameter(1)
    public String expLogin;

    /** */
    @Parameterized.Parameter(2)
    public int evtType;

    /** Parameters. */
    @Parameterized.Parameters(name = "atomicMode={0},expLogin={1},evtType={2}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[] {CacheAtomicityMode.ATOMIC, CLNT, EVT_CACHE_OBJECT_PUT},
            new Object[] {CacheAtomicityMode.ATOMIC, SRV, EVT_CACHE_OBJECT_PUT},
            new Object[] {CacheAtomicityMode.ATOMIC, "thin", EVT_CACHE_OBJECT_PUT},
            new Object[] {CacheAtomicityMode.ATOMIC, "rest", EVT_CACHE_OBJECT_PUT},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, CLNT, EVT_CACHE_OBJECT_PUT},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, SRV, EVT_CACHE_OBJECT_PUT},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, "thin", EVT_CACHE_OBJECT_PUT},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, "rest", EVT_CACHE_OBJECT_PUT},

            new Object[] {CacheAtomicityMode.ATOMIC, CLNT, EVT_CACHE_OBJECT_READ},
            new Object[] {CacheAtomicityMode.ATOMIC, SRV, EVT_CACHE_OBJECT_READ},
            new Object[] {CacheAtomicityMode.ATOMIC, "thin", EVT_CACHE_OBJECT_READ},
            new Object[] {CacheAtomicityMode.ATOMIC, "rest", EVT_CACHE_OBJECT_READ},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, CLNT, EVT_CACHE_OBJECT_READ},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, SRV, EVT_CACHE_OBJECT_READ},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, "thin", EVT_CACHE_OBJECT_READ},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, "rest", EVT_CACHE_OBJECT_READ},

            new Object[] {CacheAtomicityMode.ATOMIC, CLNT, EVT_CACHE_OBJECT_REMOVED},
            new Object[] {CacheAtomicityMode.ATOMIC, SRV, EVT_CACHE_OBJECT_REMOVED},
            new Object[] {CacheAtomicityMode.ATOMIC, "thin", EVT_CACHE_OBJECT_REMOVED},
            new Object[] {CacheAtomicityMode.ATOMIC, "rest", EVT_CACHE_OBJECT_REMOVED},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, CLNT, EVT_CACHE_OBJECT_REMOVED},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, SRV, EVT_CACHE_OBJECT_REMOVED},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, "thin", EVT_CACHE_OBJECT_REMOVED},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, "rest", EVT_CACHE_OBJECT_REMOVED},

            new Object[] {CacheAtomicityMode.TRANSACTIONAL, SRV, EVT_CACHE_OBJECT_LOCKED},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, CLNT, EVT_CACHE_OBJECT_LOCKED},

            new Object[] {CacheAtomicityMode.TRANSACTIONAL, SRV, EVT_CACHE_OBJECT_UNLOCKED},
            new Object[] {CacheAtomicityMode.TRANSACTIONAL, CLNT, EVT_CACHE_OBJECT_UNLOCKED}
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll(LISTENER_NODE);
        startGridAllowAll(SRV);
        startGridAllowAll("additional_srv");
        startClientAllowAll(CLNT);
    }

    /** */
    @Test
    public void testCacheEvent() throws Exception {
        int expTimes = evtType == EVT_CACHE_OBJECT_READ ? 1 : 3;

        evtsLatch = new CountDownLatch(2 * expTimes);

        rmtLoginCnt.set(0);
        locLoginCnt.set(0);

        final String cacheName = "test_cache_" + COUNTER.incrementAndGet();

        grid(LISTENER_NODE).createCache(new CacheConfiguration<>(cacheName)
            .setBackups(2)
            .setAtomicityMode(atomicMode));

        UUID lsnrId = grid(LISTENER_NODE).events().remoteListen(
            new IgniteBiPredicate<UUID, Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(UUID uuid, Event evt) {
                    onEvent(ign, locLoginCnt, (CacheEvent)evt, cacheName, expLogin);

                    return true;
                }
            },
            new IgnitePredicate<Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(Event evt) {
                    onEvent(ign, rmtLoginCnt, (CacheEvent)evt, cacheName, expLogin);

                    return true;
                }
            }, evtType);

        try {
            operation().accept(cacheName);
            // Waiting for events.
            evtsLatch.await(10, TimeUnit.SECONDS);

            assertEquals("Remote filter.", expTimes, rmtLoginCnt.get());
            assertEquals("Local listener.", expTimes, locLoginCnt.get());
        }
        finally {
            grid(LISTENER_NODE).events().stopRemoteListen(lsnrId);
        }
    }

    /** */
    private static void onEvent(IgniteEx ign, AtomicInteger cntr, CacheEvent evt, String cacheName, String expLogin) {
        if (cacheName.equals(evt.cacheName())) {

            cntr.incrementAndGet();

            evtsLatch.countDown();

            try {
                SecuritySubject subj = ign.context().security().authenticatedSubject(evt.subjectId());

                assertEquals(expLogin, subj.login().toString());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setIncludeEventTypes(EventType.EVTS_CACHE);
    }

    /** */
    private IgniteClient startClient() {
        return Ignition.startClient(
            new ClientConfiguration().setAddresses(Config.SERVER)
                .setUserName(expLogin)
                .setUserPassword("")
        );
    }

    /** */
    private ConsumerX<String> operation() {
        if ("thin".equals(expLogin)) {
            final IgniteClient client = startClient();

            switch (evtType) {
                case EVT_CACHE_OBJECT_PUT:
                    return n -> client.cache(n).put("key", "VALUE");

                case EVT_CACHE_OBJECT_READ:
                    return n -> {
                        client.cache(n).put("key", "VALUE");
                        client.cache(n).get("key");
                    };

                case EVT_CACHE_OBJECT_REMOVED:
                    return n -> {
                        client.cache(n).put("key", "VALUE");
                        client.cache(n).remove("key");
                    };
            }
        }
        else if ("rest".equals(expLogin)) {
            final GridRestCacheRequest req = new GridRestCacheRequest();

            req.credentials(new SecurityCredentials("rest", ""));
            req.key("key");
            req.value("VALUE");

            switch (evtType) {
                case EVT_CACHE_OBJECT_PUT:
                    return n -> {
                        req.command(GridRestCommand.CACHE_PUT);
                        req.cacheName(n);

                        restProtocolHandler().handle(req);
                    };

                case EVT_CACHE_OBJECT_READ:
                    return n -> {
                        grid(SRV).cache(n).put("key", "VALUE");

                        req.command(GridRestCommand.CACHE_GET);
                        req.cacheName(n);

                        restProtocolHandler().handle(req);
                    };

                case EVT_CACHE_OBJECT_REMOVED:
                    return n -> {
                        grid(SRV).cache(n).put("key", "VALUE");

                        req.command(GridRestCommand.CACHE_REMOVE);
                        req.cacheName(n);

                        restProtocolHandler().handle(req);
                    };
            }
        }
        else {
            final Ignite ignite = grid(expLogin);

            switch (evtType) {
                case EVT_CACHE_OBJECT_PUT:
                    return n -> ignite.cache(n).put("key", "VALUE");

                case EVT_CACHE_OBJECT_READ:
                    return n -> {
                        ignite.cache(n).put("key", "VALUE");
                        ignite.cache(n).get("key");
                    };

                case EVT_CACHE_OBJECT_REMOVED:
                    return n -> {
                        ignite.cache(n).put("key", "VALUE");
                        ignite.cache(n).remove("key");
                    };

                case EVT_CACHE_OBJECT_LOCKED:
                case EVT_CACHE_OBJECT_UNLOCKED:
                    return n -> {
                        Lock lock = grid(expLogin).cache(n).lock("key");
                        lock.lock();
                        lock.unlock();
                    };
            }
        }

        throw new RuntimeException("Event type is not processed [evtType=" + evtType + ']');
    }

    /** */
    private GridRestProtocolHandler restProtocolHandler() throws Exception{
        Object restPrc = grid(SRV).context().components().stream()
            .filter(c -> c instanceof GridRestProcessor).findFirst()
            .orElseThrow(RuntimeException::new);

        Field fld = GridRestProcessor.class.getDeclaredField("protoHnd");

        fld.setAccessible(true);

        return  (GridRestProtocolHandler)fld.get(restPrc);
    }

    /**
     * Consumer that can throw exceptions.
     */
    @FunctionalInterface
    static interface ConsumerX<T> extends Consumer<T> {
        /**
         * Consumer body.
         *
         * @throws Exception If failed.
         */
        void acceptX(T t) throws Exception;

        /** {@inheritDoc} */
        @Override default void accept(T t) {
            try {
                acceptX(t);
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }
}
