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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestProcessor;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.rest.request.RestQueryRequest;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_CREATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_DESTROYED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_UNLOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;

/**
 * Tests that an event's local listener and an event's remote filter get correct subjectId when cache's operations are
 * performed.
 */
@RunWith(Parameterized.class)
public class CacheEventsTest extends AbstractSecurityTest {
    /** Counter to name caches. */
    private static final AtomicInteger COUNTER = new AtomicInteger();

    /** Node that registers event listeners. */
    private static final String LISTENER_NODE = "listener_node";

    /** Client node. */
    private static final String CLNT = "client";

    /** Server node. */
    private static final String SRV = "server";

    /** Key. */
    private static final String KEY = "key";

    /** Initiate value. */
    public static final String INIT_VAL = "init_val";

    /** Value. */
    private static final String VAL = "val";

    /** Events latch. */
    private static CountDownLatch evtsLatch;

    /** Remote counter. */
    private static final AtomicInteger rmtCnt = new AtomicInteger();

    /** Local counter. */
    private static final AtomicInteger locCnt = new AtomicInteger();

    /** Cache atomicity mode. */
    @Parameterized.Parameter()
    public CacheAtomicityMode atomicMode;

    /** Expected login. */
    @Parameterized.Parameter(1)
    public String expLogin;

    /** Event type. */
    @Parameterized.Parameter(2)
    public int evtType;

    /** Test operation. */
    @Parameterized.Parameter(3)
    public TestOperation op;

    /** Parameters. */
    @Parameterized.Parameters(name = "atomicMode={0}, expLogin={1}, evtType={2}, op={3}")
    public static Iterable<Object[]> data() {
        List<Object[]> lst = Arrays.asList(
            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.PUT},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.PUT},

            new Object[] {CLNT, EVT_CACHE_ENTRY_CREATED, TestOperation.PUT},
            new Object[] {SRV, EVT_CACHE_ENTRY_CREATED, TestOperation.PUT},
            new Object[] {"thin", EVT_CACHE_ENTRY_CREATED, TestOperation.PUT},
            new Object[] {"rest", EVT_CACHE_ENTRY_CREATED, TestOperation.PUT},

            new Object[] {CLNT, EVT_CACHE_ENTRY_DESTROYED, TestOperation.PUT},
            new Object[] {SRV, EVT_CACHE_ENTRY_DESTROYED, TestOperation.PUT},
            new Object[] {"thin", EVT_CACHE_ENTRY_DESTROYED, TestOperation.PUT},
            new Object[] {"rest", EVT_CACHE_ENTRY_DESTROYED, TestOperation.PUT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_ALL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT_ASYNC},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.PUT_IF_ABSENT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET},
            new Object[] {"thin", EVT_CACHE_OBJECT_READ, TestOperation.GET},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRY},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRY},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRY_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRY_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRIES},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRIES},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRIES_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ENTRIES_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL},
            new Object[] {"thin", EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_ALL_OUT_TX_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT},
            new Object[] {"thin", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_PUT_IF_ABSENT_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_PUT_IF_ABSENT_PUT_CASE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE},
            new Object[] {"thin", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE},
            new Object[] {"thin", EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE},
            new Object[] {"rest", EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REMOVE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.GET_AND_REMOVE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE},
            new Object[] {"thin", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE},
            new Object[] {"rest", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL},
            new Object[] {"thin", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL},
            new Object[] {"rest", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_VAL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL},
            new Object[] {"thin", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL},
            new Object[] {"rest", EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_REMOVED, TestOperation.REMOVE_ALL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.REPLACE_VAL_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE},
            new Object[] {"thin", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE},
            new Object[] {"rest", EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_READ, TestOperation.GET_AND_REPLACE_ASYNC},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE},
            new Object[] {"thin", EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE},
            new Object[] {"rest", EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE},

            new Object[] {CLNT, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE_ASYNC},
            new Object[] {SRV, EVT_CACHE_OBJECT_PUT, TestOperation.GET_AND_REPLACE_ASYNC},

            new Object[] {SRV, EVT_CACHE_OBJECT_LOCKED, TestOperation.LOCK},
            new Object[] {CLNT, EVT_CACHE_OBJECT_LOCKED, TestOperation.LOCK},
            new Object[] {SRV, EVT_CACHE_OBJECT_UNLOCKED, TestOperation.LOCK},
            new Object[] {CLNT, EVT_CACHE_OBJECT_UNLOCKED, TestOperation.LOCK},

            new Object[] {SRV, EVT_CACHE_OBJECT_LOCKED, TestOperation.LOCK_ALL},
            new Object[] {CLNT, EVT_CACHE_OBJECT_LOCKED, TestOperation.LOCK_ALL},
            new Object[] {SRV, EVT_CACHE_OBJECT_UNLOCKED, TestOperation.LOCK_ALL},
            new Object[] {CLNT, EVT_CACHE_OBJECT_UNLOCKED, TestOperation.LOCK_ALL},

            new Object[] {CLNT, EVT_CACHE_QUERY_EXECUTED, TestOperation.SCAN_QUERY},
            new Object[] {SRV, EVT_CACHE_QUERY_EXECUTED, TestOperation.SCAN_QUERY},
            new Object[] {"thin", EVT_CACHE_QUERY_EXECUTED, TestOperation.SCAN_QUERY},
            new Object[] {"rest", EVT_CACHE_QUERY_EXECUTED, TestOperation.SCAN_QUERY},

            new Object[] {CLNT, EVT_CACHE_QUERY_OBJECT_READ, TestOperation.SCAN_QUERY},
            new Object[] {SRV, EVT_CACHE_QUERY_OBJECT_READ, TestOperation.SCAN_QUERY},
            new Object[] {"thin", EVT_CACHE_QUERY_OBJECT_READ, TestOperation.SCAN_QUERY},
            new Object[] {"rest", EVT_CACHE_QUERY_OBJECT_READ, TestOperation.SCAN_QUERY}
        );

        List<Object[]> res = new ArrayList<>();

        for (Object[] arr : lst) {
            res.add(new Object[] {CacheAtomicityMode.TRANSACTIONAL, arr[0], arr[1], arr[2]});

            int evt = (int)arr[1];

            TestOperation op = (TestOperation)arr[2];
            // todo IGNITE-13490 Cache's operation getAndXXX doesn't trigger a CacheEvent with type EVT_CACHE_OBJECT_READ.
            // This condition should be removed when the issue will be done.
            if (evt == EVT_CACHE_OBJECT_READ && op.name().contains("GET_AND_"))
                continue;

            if (evt == EVT_CACHE_OBJECT_LOCKED || evt == EVT_CACHE_OBJECT_UNLOCKED)
                continue;

            res.add(new Object[] {CacheAtomicityMode.ATOMIC, arr[0], arr[1], arr[2]});
        }

        return res;
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
        int expTimes = evtType == EVT_CACHE_OBJECT_READ || evtType == EVT_CACHE_QUERY_OBJECT_READ ? 1 : 3;

        evtsLatch = new CountDownLatch(2 * expTimes);

        rmtCnt.set(0);
        locCnt.set(0);

        final String cacheName = "test_cache_" + COUNTER.incrementAndGet();

        IgniteCache<String, String> cache = grid(LISTENER_NODE).createCache(
            new CacheConfiguration<String, String>(cacheName)
                .setBackups(2)
                .setAtomicityMode(atomicMode));

        if (op.valIsRequired)
            cache.put(KEY, INIT_VAL);

        UUID lsnrId = grid(LISTENER_NODE).events().remoteListen(
            new IgniteBiPredicate<UUID, Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(UUID uuid, Event evt) {
                    onEvent(ign, locCnt, new TestEventAdapter(evt), cacheName, expLogin);

                    return true;
                }
            },
            new IgnitePredicate<Event>() {
                @IgniteInstanceResource IgniteEx ign;

                @Override public boolean apply(Event evt) {
                    onEvent(ign, rmtCnt, new TestEventAdapter(evt), cacheName, expLogin);

                    return true;
                }
            }, evtType);

        try {
            operation().accept(cacheName);
            // Waiting for events.
            evtsLatch.await(10, TimeUnit.SECONDS);

            assertEquals("Remote filter.", expTimes, rmtCnt.get());
            assertEquals("Local listener.", expTimes, locCnt.get());
        }
        finally {
            grid(LISTENER_NODE).events().stopRemoteListen(lsnrId);
        }
    }

    /**
     *
     */
    private static void onEvent(IgniteEx ign, AtomicInteger cntr, TestEventAdapter evt, String cacheName,
        String expLogin) {
        if (cacheName.equals(evt.cacheName()) &&
            (evt.type() != EVT_CACHE_OBJECT_PUT || !F.eq(INIT_VAL, evt.newValue()))) {

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
            .setIncludeEventTypes(EVT_CACHE_ENTRY_CREATED,
                EVT_CACHE_ENTRY_DESTROYED,
                EVT_CACHE_OBJECT_PUT,
                EVT_CACHE_OBJECT_READ,
                EVT_CACHE_OBJECT_REMOVED,
                EVT_CACHE_OBJECT_LOCKED,
                EVT_CACHE_OBJECT_UNLOCKED,
                EVT_CACHE_QUERY_EXECUTED,
                EVT_CACHE_QUERY_OBJECT_READ);
    }

    /** */
    private ConsumerX<String> operation() {
        if ("rest".equals(expLogin)) {
            if (op == TestOperation.SCAN_QUERY) {
                RestQueryRequest req = new RestQueryRequest();

                req.credentials(new SecurityCredentials("rest", ""));
                req.command(op.restCmd);
                req.queryType(RestQueryRequest.QueryType.SCAN);
                req.pageSize(256);

                return n -> {
                    req.cacheName(n);

                    restProtocolHandler().handle(req);
                };
            }
            else {
                final GridRestCacheRequest req = new GridRestCacheRequest();

                req.credentials(new SecurityCredentials("rest", ""));
                req.key(KEY);
                req.command(op.restCmd);

                if (op == TestOperation.REPLACE_VAL) {
                    req.value(VAL);
                    req.value2(INIT_VAL);
                }
                else if (op == TestOperation.REMOVE_VAL)
                    req.value(INIT_VAL);
                else {
                    req.value(VAL);
                    req.values(F.asMap(KEY, VAL));
                }

                return n -> {
                    req.cacheName(n);

                    restProtocolHandler().handle(req);
                };
            }
        }
        else if ("thin".equals(expLogin))
            return n -> op.clntCacheOp.accept(startClient().cache(n));
        else
            return n -> op.ignCacheOp.accept(grid(expLogin).cache(n));
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
    private GridRestProtocolHandler restProtocolHandler() throws Exception {
        Object restPrc = grid(LISTENER_NODE).context().components().stream()
            .filter(c -> c instanceof GridRestProcessor).findFirst()
            .orElseThrow(RuntimeException::new);

        Field fld = GridRestProcessor.class.getDeclaredField("protoHnd");

        fld.setAccessible(true);

        return (GridRestProtocolHandler)fld.get(restPrc);
    }

    /** Test opeartions. */
    private enum TestOperation {
        /** Put. */
        PUT(c -> c.put(KEY, VAL), c -> c.put(KEY, VAL), GridRestCommand.CACHE_PUT, false),

        /** Put async. */
        PUT_ASYNC(c -> c.putAsync(KEY, VAL).get(), false),

        /** Put all. */
        PUT_ALL(c -> c.putAll(F.asMap(KEY, VAL)), c -> c.putAll(F.asMap(KEY, VAL)), GridRestCommand.CACHE_PUT_ALL, false),

        /** Put all async. */
        PUT_ALL_ASYNC(c -> c.putAll(F.asMap(KEY, VAL)), false),

        /** Put if absent. */
        PUT_IF_ABSENT(c -> c.putIfAbsent(KEY, VAL), c -> c.putIfAbsent(KEY, VAL), GridRestCommand.CACHE_PUT_IF_ABSENT, false),

        /** Put if absent async. */
        PUT_IF_ABSENT_ASYNC(c -> c.putIfAbsentAsync(KEY, VAL), null, GridRestCommand.CACHE_ADD, false),

        /** Get. */
        GET(c -> c.get(KEY), c -> c.get(KEY), GridRestCommand.CACHE_GET),

        /** Get async. */
        GET_ASYNC(c -> c.getAsync(KEY).get()),

        /** Get entry. */
        GET_ENTRY(c -> c.getEntry(KEY)),

        /** Get entry async. */
        GET_ENTRY_ASYNC(c -> c.getEntryAsync(KEY).get()),

        /** Get entries. */
        GET_ENTRIES(c -> c.getEntries(Collections.singleton(KEY))),

        /** Get entries async. */
        GET_ENTRIES_ASYNC(c -> c.getEntriesAsync(Collections.singleton(KEY))),

        /** Get all. */
        GET_ALL(c -> c.getAll(Collections.singleton(KEY)), c -> c.getAll(Collections.singleton(KEY)), GridRestCommand.CACHE_GET_ALL),

        /** Get all async. */
        GET_ALL_ASYNC(c -> c.getAllAsync(Collections.singleton(KEY)).get()),

        /** Get all out tx. */
        GET_ALL_OUT_TX(c -> c.getAllOutTx(Collections.singleton(KEY))),

        /** Get all out tx async. */
        GET_ALL_OUT_TX_ASYNC(c -> c.getAllOutTxAsync(Collections.singleton(KEY)).get()),

        /** Get and put. */
        GET_AND_PUT(c -> c.getAndPut(KEY, VAL), c -> c.getAndPut(KEY, VAL), GridRestCommand.CACHE_GET_AND_PUT),

        /** Get and put async. */
        GET_AND_PUT_ASYNC(c -> c.getAndPutAsync(KEY, VAL).get()),

        /** Get and put if absent. */
        GET_AND_PUT_IF_ABSENT(c -> c.getAndPutIfAbsent(KEY, VAL), null, GridRestCommand.CACHE_GET_AND_PUT_IF_ABSENT),

        /** Get and put if absent async. */
        GET_AND_PUT_IF_ABSENT_ASYNC(c -> c.getAndPutIfAbsentAsync(KEY, VAL).get()),

        /** Get and put if absent put case. */
        GET_AND_PUT_IF_ABSENT_PUT_CASE(c -> c.getAndPutIfAbsent(KEY, VAL), null, GridRestCommand.CACHE_GET_AND_PUT_IF_ABSENT, false),

        /** Get and put if absent put case async. */
        GET_AND_PUT_IF_ABSENT_PUT_CASE_ASYNC(c -> c.getAndPutIfAbsentAsync(KEY, VAL).get(), false),

        /** Remove. */
        REMOVE(c -> c.remove(KEY), c -> c.remove(KEY), GridRestCommand.CACHE_REMOVE),

        /** Remove async. */
        REMOVE_ASYNC(c -> c.removeAsync(KEY).get()),

        /** Remove value. */
        REMOVE_VAL(c -> c.remove(KEY, INIT_VAL), c -> c.remove(KEY, INIT_VAL), GridRestCommand.CACHE_REMOVE_VALUE),

        /** Remove value async. */
        REMOVE_VAL_ASYNC(c -> c.removeAsync(KEY, INIT_VAL).get()),

        /** Get and remove. */
        GET_AND_REMOVE(c -> c.getAndRemove(KEY), c -> c.getAndRemove(KEY), GridRestCommand.CACHE_GET_AND_REMOVE),

        /** Get and remove async. */
        GET_AND_REMOVE_ASYNC(c -> c.getAndRemoveAsync(KEY).get()),

        /** Remove all. */
        REMOVE_ALL(c -> c.removeAll(Collections.singleton(KEY)), c -> c.removeAll(Collections.singleton(KEY)), GridRestCommand.CACHE_REMOVE_ALL),

        /** Remove all async. */
        REMOVE_ALL_ASYNC(c -> c.removeAllAsync(Collections.singleton(KEY)).get()),

        /** Replace. */
        REPLACE(c -> c.replace(KEY, VAL), c -> c.replace(KEY, VAL), GridRestCommand.CACHE_REPLACE, true),

        /** Replace async. */
        REPLACE_ASYNC(c -> c.replaceAsync(KEY, VAL).get(), true),

        /** Replace value. */
        REPLACE_VAL(c -> c.replace(KEY, INIT_VAL, VAL), c -> c.replace(KEY, INIT_VAL, VAL), GridRestCommand.CACHE_REPLACE_VALUE),

        /** Replace value async. */
        REPLACE_VAL_ASYNC(c -> c.replaceAsync(KEY, INIT_VAL, VAL).get()),

        /** Get and replace. */
        GET_AND_REPLACE(c -> c.getAndReplace(KEY, VAL), c -> c.getAndReplace(KEY, VAL), GridRestCommand.CACHE_GET_AND_REPLACE),

        /** Get and replace async. */
        GET_AND_REPLACE_ASYNC(c -> c.getAndReplaceAsync(KEY, VAL).get()),

        /** Lock. */
        LOCK(c -> {
            Lock lock = c.lock(KEY);

            lock.lock();
            lock.unlock();
        }, false),

        /** Lock all. */
        LOCK_ALL(c -> {
            Lock lock = c.lockAll(Collections.singleton(KEY));

            lock.lock();
            lock.unlock();
        }, false),

        /** Scan query. */
        SCAN_QUERY(
            c -> {
                try (QueryCursor<Cache.Entry<String, String>> cursor = c.query(new ScanQuery<>())) {
                    cursor.getAll();
                }
            },
            c -> {
                try (QueryCursor<Cache.Entry<String, String>> cursor = c.query(new ScanQuery<>())) {
                    cursor.getAll();
                }
            },
            GridRestCommand.EXECUTE_SCAN_QUERY);

        /** IgniteCache operation. */
        final Consumer<IgniteCache<String, String>> ignCacheOp;

        /** ClientCache operation. */
        final Consumer<ClientCache<String, String>> clntCacheOp;

        /** Rest client command. */
        final GridRestCommand restCmd;

        /**
         * True if a test operation requires existence of value in a cache.
         */
        final boolean valIsRequired;

        /**
         * @param ignCacheOp IgniteCache operation.
         */
        TestOperation(Consumer<IgniteCache<String, String>> ignCacheOp) {
            this(ignCacheOp, null, null, true);
        }

        /**
         * @param ignCacheOp IgniteCache operation.
         */
        TestOperation(Consumer<IgniteCache<String, String>> ignCacheOp, boolean valIsRequired) {
            this(ignCacheOp, null, null, valIsRequired);
        }

        /**
         * @param ignCacheOp IgniteCache operation.
         * @param clntCacheOp ClientCache operation.
         * @param restCmd Rest client command.
         */
        TestOperation(Consumer<IgniteCache<String, String>> ignCacheOp,
            Consumer<ClientCache<String, String>> clntCacheOp,
            GridRestCommand restCmd) {
            this(ignCacheOp, clntCacheOp, restCmd, true);
        }

        /**
         * @param ignCacheOp IgniteCache operation.
         * @param clntCacheOp ClientCache operation.
         * @param restCmd Rest client command.
         * @param valIsRequired Test operation requires existence of value in a cache.
         */
        TestOperation(Consumer<IgniteCache<String, String>> ignCacheOp,
            Consumer<ClientCache<String, String>> clntCacheOp,
            GridRestCommand restCmd,
            boolean valIsRequired) {
            this.ignCacheOp = ignCacheOp;
            this.clntCacheOp = clntCacheOp;
            this.restCmd = restCmd;
            this.valIsRequired = valIsRequired;
        }
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

    /**
     * Test event's adapter.
     */
    static class TestEventAdapter {
        /** CacheEvent. */
        private final CacheEvent cacheEvt;

        /** CacheQueryExecutedEvent. */
        private final CacheQueryExecutedEvent<String, String> qryExecEvt;

        /** CacheQueryReadEvent. */
        private final CacheQueryReadEvent<String, String> qryReadEvt;

        /**
         * @param evt Event.
         */
        TestEventAdapter(Event evt) {
            if (evt instanceof CacheEvent) {
                cacheEvt = (CacheEvent)evt;
                qryReadEvt = null;
                qryExecEvt = null;
            }
            else if (evt instanceof CacheQueryExecutedEvent) {
                qryExecEvt = (CacheQueryExecutedEvent<String, String>)evt;
                cacheEvt = null;
                qryReadEvt = null;
            }
            else if (evt instanceof CacheQueryReadEvent) {
                qryReadEvt = (CacheQueryReadEvent<String, String>)evt;
                cacheEvt = null;
                qryExecEvt = null;
            }
            else
                throw new IllegalArgumentException("Unexpected event=[" + evt + "]");
        }

        /**
         * @return Event's type.
         */
        int type() {
            if (cacheEvt != null)
                return cacheEvt.type();

            if (qryExecEvt != null)
                return qryExecEvt.type();

            return qryReadEvt.type();
        }

        /**
         * @return Event's cache name.
         */
        String cacheName() {
            if (cacheEvt != null)
                return cacheEvt.cacheName();

            if (qryExecEvt != null)
                return qryExecEvt.cacheName();

            return qryReadEvt.cacheName();
        }

        /**
         * @return Event's subject id.
         */
        UUID subjectId() {
            if (cacheEvt != null)
                return cacheEvt.subjectId();

            if (qryExecEvt != null)
                return qryExecEvt.subjectId();

            return qryReadEvt.subjectId();
        }

        /**
         * @return Event's new value.
         */
        Object newValue() {
            return cacheEvt != null ? cacheEvt.newValue() : null;
        }
    }
}
