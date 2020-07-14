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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Test that an event's local listener and an event's remote filter get correct subjectId
 * when a server (client) node create or destroy a cache.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@RunWith(Parameterized.class)
public class CacheCreateDestroyEventsTest extends AbstractSecurityTest {
    /** Counter. */
    protected static final AtomicInteger COUNTER = new AtomicInteger();

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
    public int opNum;

    /** Parameters. */
    @Parameterized.Parameters(name = "cacheCnt={0},evtNode={1},evtType={2},opNum={3}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[] {1, SRV, EVT_CACHE_STARTED, 0},
            new Object[] {1, CLNT, EVT_CACHE_STARTED, 0},
            new Object[] {1, SRV, EVT_CACHE_STARTED, 1},
            new Object[] {1, CLNT, EVT_CACHE_STARTED, 1},
            new Object[] {1, SRV, EVT_CACHE_STOPPED, 2},
            new Object[] {1, CLNT, EVT_CACHE_STOPPED, 2},
            new Object[] {2, SRV, EVT_CACHE_STARTED, 3},
            new Object[] {2, CLNT, EVT_CACHE_STARTED, 3},
            new Object[] {2, SRV, EVT_CACHE_STOPPED, 4},
            new Object[] {2, CLNT, EVT_CACHE_STOPPED, 4},
            new Object[] {1, "thin", EVT_CACHE_STARTED, 5},
            new Object[] {1, "thin", EVT_CACHE_STARTED, 6},
            new Object[] {1, "thin", EVT_CACHE_STOPPED, 7},
            new Object[] {3, "new_client_node", EVT_CACHE_STARTED, 8},
            new Object[] {3, "new_server_node", EVT_CACHE_STARTED, 8},
            new Object[] {2, SRV, EVT_CACHE_STARTED, 9},
            new Object[] {2, CLNT, EVT_CACHE_STARTED, 9},
            new Object[] {2, SRV, EVT_CACHE_STOPPED, 9},
            new Object[] {2, CLNT, EVT_CACHE_STOPPED, 9}
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
    private List<Consumer<Collection<CacheConfiguration>>> operations() {
        return Arrays.asList(
            ccfgs -> grid(login).getOrCreateCache(ccfgs.iterator().next()), //0
            ccfgs -> grid(login).createCache(ccfgs.iterator().next()), //1
            ccfgs -> grid(login).destroyCache(ccfgs.iterator().next().getName()), //2
            ccfgs -> grid(login).createCaches(ccfgs), //3
            ccfgs -> grid(login).destroyCaches(ccfgs.stream().map(CacheConfiguration::getName).collect(Collectors.toSet())),//4
            ccfgs -> startClient().createCache(ccfgs.iterator().next().getName()), //5
            ccfgs -> startClient().getOrCreateCache(ccfgs.iterator().next().getName()), //6
            ccfgs -> startClient().destroyCache(ccfgs.iterator().next().getName()), //7
            ccfgs -> { //8
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
            },
            ccfgs -> { //9
                grid(login).cluster().state(ClusterState.INACTIVE);

                grid(login).cluster().state(ClusterState.ACTIVE);
            }
        );
    }

    /** */
    @Test
    public void testDynamicCreateDestroyCache() throws Exception {
        int expTimes = cacheCnt * (opNum != 9 && (SRV.equals(login) || "thin".equals(login)) ? 2 : 3);

        Collection<CacheConfiguration> ccfgs = new ArrayList<>(cacheCnt);

        for (int i = 0; i < cacheCnt; i++)
            ccfgs.add(new CacheConfiguration("test_cache_" + COUNTER.incrementAndGet()));

        if (opNum == 9 || evtType == EVT_CACHE_STOPPED) {
            ccfgs.forEach(c -> grid(LISTENER_NODE).createCache(c.getName()));

            if (opNum == 9 || CLNT.equals(login))
                ccfgs.forEach(c -> grid(CLNT).cache(c.getName()));
        }

        locLoginCnt.set(0);
        rmtLoginCnt.set(0);

        evtsLatch = new CountDownLatch(2 * expTimes);

        UUID lsnrId = grid(LISTENER_NODE).events().remoteListen(new IgniteBiPredicate<UUID, Event>() {
            @Override public boolean apply(UUID uuid, Event evt) {
                if (onEvent((CacheEvent)evt, ccfgs, login))
                    locLoginCnt.incrementAndGet();

                return true;
            }
        }, new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (onEvent((CacheEvent)evt, ccfgs, login))
                    rmtLoginCnt.incrementAndGet();

                return true;
            }
        }, evtType);

        try {
            // Execute tested operation.
            operations().get(opNum).accept(ccfgs);

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
        return super.getConfiguration(igniteInstanceName).setIncludeEventTypes(EventType.EVTS_CACHE_LIFECYCLE);
    }

    /** */
    private static boolean onEvent(CacheEvent evt, Collection<CacheConfiguration> ccfgs, String expLogin) {
        if (ccfgs.stream().noneMatch(ccfg -> ccfg.getName().equals(evt.cacheName())))
            return false;

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

        return true;
    }
}
