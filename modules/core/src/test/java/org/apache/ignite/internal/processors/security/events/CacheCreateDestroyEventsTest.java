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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.request.GridRestCacheRequest;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.DESTROY_CACHE;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.GET_OR_CREATE_CACHE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Test that an event's local listener and an event's remote filter get correct subjectId
 * when a server (client) node create or destroy a cache.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@RunWith(Parameterized.class)
public class CacheCreateDestroyEventsTest extends AbstractSecurityTest {
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

    /** */
    private static final AtomicInteger rmtLoginCnt = new AtomicInteger();

    /** */
    private static final AtomicInteger locLoginCnt = new AtomicInteger();

    /** */
    @Parameterized.Parameter()
    public int cacheCnt;

    /** */
    @Parameterized.Parameter(1)
    public String login;

    /** */
    @Parameterized.Parameter(2)
    public int evtType;

    /** */
    @Parameterized.Parameter(3)
    public TestOperation op;

    /** Parameters. */
    @Parameterized.Parameters(name = "cacheCnt={0}, evtNode={1}, evtType={2}, op={3}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[] {1, SRV, EVT_CACHE_STARTED, TestOperation.GET_OR_CREATE_CACHE},
            new Object[] {1, CLNT, EVT_CACHE_STARTED, TestOperation.GET_OR_CREATE_CACHE},
            new Object[] {1, SRV, EVT_CACHE_STARTED, TestOperation.CREATE_CACHE},
            new Object[] {1, CLNT, EVT_CACHE_STARTED, TestOperation.CREATE_CACHE},
            new Object[] {1, SRV, EVT_CACHE_STOPPED, TestOperation.DESTROY_CACHE},
            new Object[] {1, CLNT, EVT_CACHE_STOPPED, TestOperation.DESTROY_CACHE},
            new Object[] {2, SRV, EVT_CACHE_STARTED, TestOperation.CREATE_CACHES},
            new Object[] {2, CLNT, EVT_CACHE_STARTED, TestOperation.CREATE_CACHES},
            new Object[] {2, SRV, EVT_CACHE_STOPPED, TestOperation.DESTROY_CACHES},
            new Object[] {2, CLNT, EVT_CACHE_STOPPED, TestOperation.DESTROY_CACHES},
            new Object[] {1, "thin", EVT_CACHE_STARTED, TestOperation.THIN_CLIENT_CREATE_CACHE},
            new Object[] {1, "thin", EVT_CACHE_STARTED, TestOperation.THIN_CLIENT_GET_OR_CREATE},
            new Object[] {1, "thin", EVT_CACHE_STOPPED, TestOperation.THIN_CLIENT_DESTROY_CACHE},
            new Object[] {3, "new_client_node", EVT_CACHE_STARTED, TestOperation.START_NODE},
            new Object[] {3, "new_server_node", EVT_CACHE_STARTED, TestOperation.START_NODE},
            new Object[] {2, SRV, EVT_CACHE_STARTED, TestOperation.CHANGE_CLUSTER_STATE},
            new Object[] {2, CLNT, EVT_CACHE_STARTED, TestOperation.CHANGE_CLUSTER_STATE},
            new Object[] {2, SRV, EVT_CACHE_STOPPED, TestOperation.CHANGE_CLUSTER_STATE},
            new Object[] {2, CLNT, EVT_CACHE_STOPPED, TestOperation.CHANGE_CLUSTER_STATE},
            new Object[] {1, "rest", EVT_CACHE_STARTED, TestOperation.REST_GET_OR_CREATE_CACHE},
            new Object[] {1, "rest", EVT_CACHE_STOPPED, TestOperation.REST_DESTROY_CACHE}
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll(LISTENER_NODE);
        startGridAllowAll(SRV);
        startClientAllowAll(CLNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid("new_client_node");
        stopGrid("new_server_node");
    }

    /** */
    private GridTestUtils.ConsumerX<Collection<CacheConfiguration>> operation() {
        switch (op) {
            case GET_OR_CREATE_CACHE:
                return ccfgs -> grid(login).getOrCreateCache(ccfgs.iterator().next());

            case CREATE_CACHE:
                return ccfgs -> grid(login).createCache(ccfgs.iterator().next());

            case DESTROY_CACHE:
                return ccfgs -> grid(login).destroyCache(ccfgs.iterator().next().getName());

            case CREATE_CACHES:
                return ccfgs -> grid(login).createCaches(ccfgs);

            case DESTROY_CACHES:
                return ccfgs -> grid(login).destroyCaches(ccfgs.stream().map(CacheConfiguration::getName).collect(Collectors.toSet()));

            case THIN_CLIENT_GET_OR_CREATE:
                return ccfgs -> {
                    try (IgniteClient clnt = startClient()) {
                        clnt.getOrCreateCache(ccfgs.iterator().next().getName());
                    }
                };

            case THIN_CLIENT_CREATE_CACHE:
                return ccfgs -> {
                    try (IgniteClient clnt = startClient()) {
                        clnt.createCache(ccfgs.iterator().next().getName());
                    }
                };

            case THIN_CLIENT_DESTROY_CACHE:
                return ccfgs -> {
                    try (IgniteClient clnt = startClient()) {
                        clnt.destroyCache(ccfgs.iterator().next().getName());
                    }
                };

            case START_NODE:
                return ccfgs -> {
                    try {
                        startGrid(getConfiguration(login,
                            new TestSecurityPluginProvider(login, "", ALLOW_ALL, false))
                            .setClientMode(login.contains("client"))
                            .setCacheConfiguration(ccfgs.toArray(
                                new CacheConfiguration[] {new CacheConfiguration("test_cache_" + COUNTER.incrementAndGet())}))
                        );
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };

            case CHANGE_CLUSTER_STATE:
                return ccfgs -> {
                    grid(login).cluster().state(ClusterState.INACTIVE);

                    grid(login).cluster().state(ClusterState.ACTIVE);
                };

            case REST_GET_OR_CREATE_CACHE:
                return ccfgs -> handleRestRequest(GET_OR_CREATE_CACHE, ccfgs.iterator().next().getName());

            case REST_DESTROY_CACHE:
                return ccfgs -> handleRestRequest(DESTROY_CACHE, ccfgs.iterator().next().getName());

            default:
                throw new IllegalArgumentException("Unknown operation " + op);
        }
    }

    /** */
    private void handleRestRequest(GridRestCommand cmd, String cacheName) {
        final GridRestCacheRequest req = new GridRestCacheRequest();

        req.command(cmd);
        req.credentials(new SecurityCredentials("rest", ""));
        req.cacheName(cacheName);

        try {
            restProtocolHandler(grid(SRV)).handle(req);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Create/destroy cach events should contain relevant subject ID.
     *
     * @throws Exception if fail.
     */
    @Test
    public void testDynamicCreateDestroyCache() throws Exception {
        int expTimes = cacheCnt * (op != TestOperation.CHANGE_CLUSTER_STATE &&
            (SRV.equals(login) || "thin".equals(login) || "rest".equals(login)) ? 2 : 3);

        Collection<CacheConfiguration> ccfgs = new ArrayList<>(cacheCnt);

        for (int i = 0; i < cacheCnt; i++)
            ccfgs.add(new CacheConfiguration("test_cache_" + COUNTER.incrementAndGet()));

        if (op == TestOperation.CHANGE_CLUSTER_STATE || evtType == EVT_CACHE_STOPPED) {
            ccfgs.forEach(c -> grid(LISTENER_NODE).createCache(c.getName()));

            if (op == TestOperation.CHANGE_CLUSTER_STATE || CLNT.equals(login))
                ccfgs.forEach(c -> grid(CLNT).cache(c.getName()));
        }

        locLoginCnt.set(0);
        rmtLoginCnt.set(0);

        evtsLatch = new CountDownLatch(2 * expTimes);

        UUID lsnrId = grid(LISTENER_NODE).events().remoteListen(new IgniteBiPredicate<UUID, Event>() {
            @Override public boolean apply(UUID uuid, Event evt) {
                onEvent((CacheEvent)evt, locLoginCnt, ccfgs, login);

                return true;
            }
        }, new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                onEvent((CacheEvent)evt, rmtLoginCnt, ccfgs, login);

                return true;
            }
        }, evtType);

        try {
            // Execute tested operation.
            operation().accept(ccfgs);

            // Waiting for events.
            evtsLatch.await(10, TimeUnit.SECONDS);

            assertEquals("Remote filter.", expTimes, rmtLoginCnt.get());
            assertEquals("Local listener.", expTimes, locLoginCnt.get());
        }
        finally {
            grid(LISTENER_NODE).events().stopRemoteListenAsync(lsnrId).get();
        }
    }

    /**
     * @return Thin client for specified user.
     */
    private IgniteClient startClient() {
        return Ignition.startClient(
            new ClientConfiguration().setAddresses(Config.SERVER)
                .setUserName(login)
                .setUserPassword("")
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setIncludeEventTypes(EventType.EVTS_CACHE_LIFECYCLE);
    }

    /** */
    private static void onEvent(CacheEvent evt, AtomicInteger cntr, Collection<CacheConfiguration> ccfgs, String expLogin) {
        if (ccfgs.stream().noneMatch(ccfg -> ccfg.getName().equals(evt.cacheName())))
            return;

        cntr.incrementAndGet();

        evtsLatch.countDown();

        try {
            SecuritySubject subj = IgnitionEx.localIgnite().context().security()
                .authenticatedSubject(evt.subjectId());

            String login = subj.login().toString();

            assertEquals(expLogin, login);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** Test operations. */
    enum TestOperation {
        /** Get or create cache. */
        GET_OR_CREATE_CACHE,

        /** Create cache. */
        CREATE_CACHE,

        /** Destroy cache. */
        DESTROY_CACHE,

        /** Create caches. */
        CREATE_CACHES,

        /** Destroy caches. */
        DESTROY_CACHES,

        /** Thin client get or create. */
        THIN_CLIENT_GET_OR_CREATE,

        /** Thin client create cache. */
        THIN_CLIENT_CREATE_CACHE,

        /** Thin client destroy cache. */
        THIN_CLIENT_DESTROY_CACHE,

        /** Start node. */
        START_NODE,

        /** Change cluster state. */
        CHANGE_CLUSTER_STATE,

        /** Rest get or create cache. */
        REST_GET_OR_CREATE_CACHE,

        /** Rest destroy cache. */
        REST_DESTROY_CACHE;
    }
}
