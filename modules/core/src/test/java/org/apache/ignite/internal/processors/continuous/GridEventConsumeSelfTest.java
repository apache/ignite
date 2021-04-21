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

package org.apache.ignite.internal.processors.continuous;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVTS_ALL;
import static org.apache.ignite.events.EventType.EVTS_DISCOVERY;
import static org.apache.ignite.events.EventType.EVT_JOB_FINISHED;
import static org.apache.ignite.events.EventType.EVT_JOB_STARTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.LocalRoutineInfo;

/**
 * Event consume test.
 */
public class GridEventConsumeSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String PRJ_PRED_CLS_NAME = "org.apache.ignite.tests.p2p.GridEventConsumeProjectionPredicate";

    /** */
    private static final String FILTER_CLS_NAME = "org.apache.ignite.tests.p2p.GridEventConsumeFilter";

    /** Grids count. */
    private static final int GRID_CNT = 3;

    /** Number of created consumes per thread in multithreaded test. */
    private static final int CONSUME_CNT = 500;

    /** Consume latch. */
    private static volatile CountDownLatch consumeLatch;

    /** Consume counter. */
    private static volatile AtomicInteger consumeCnt;

    /** Include node flag. */
    private boolean include;

    /** No automatic unsubscribe flag. */
    private boolean noAutoUnsubscribe;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        if (include)
            cfg.setUserAttributes(F.asMap("include", true));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        assertTrue(GRID_CNT > 1);

        include = true;

        startGrids(GRID_CNT - 1);

        include = false;

        startGrid(GRID_CNT - 1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            assertTrue(GRID_CNT >= grid(0).cluster().nodes().size());

            for (Ignite ignite : G.allGrids()) {
                IgniteEx grid = (IgniteEx) ignite;

                GridContinuousProcessor proc = grid.context().continuous();

                try {
                    if (!noAutoUnsubscribe) {
                        Map rmtInfos = U.field(proc, "rmtInfos");

                        assertTrue("Unexpected remote infos: " + rmtInfos, rmtInfos.isEmpty());
                    }
                }
                finally {
                    U.<Map>field(proc, "rmtInfos").clear();
                }

                assertEquals(0, U.<Map>field(proc, "rmtInfos").size());
                assertEquals(0, U.<Map>field(proc, "startFuts").size());
                assertEquals(0, U.<Map>field(proc, "stopFuts").size());
                assertEquals(0, U.<Map>field(proc, "bufCheckThreads").size());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param proc Continuous processor.
     * @return Local event routines.
     */
    private Collection<LocalRoutineInfo> localRoutines(GridContinuousProcessor proc) {
        return F.view(U.<Map<UUID, LocalRoutineInfo>>field(proc, "locInfos").values(),
            new IgnitePredicate<LocalRoutineInfo>() {
                @Override public boolean apply(LocalRoutineInfo info) {
                    return info.handler().isEvents();
                }
            });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testApi() throws Exception {
        try {
            grid(0).events().stopRemoteListen(null);
        }
        catch (NullPointerException ignored) {
            // No-op.
        }

        grid(0).events().stopRemoteListen(UUID.randomUUID());

        UUID consumeId = null;

        try {
            consumeId = grid(0).events().remoteListen(
                new P2<UUID, DiscoveryEvent>() {
                    @Override public boolean apply(UUID uuid, DiscoveryEvent evt) {
                        return false;
                    }
                },
                new P1<DiscoveryEvent>() {
                    @Override public boolean apply(DiscoveryEvent e) {
                        return false;
                    }
                },
                EVTS_DISCOVERY
            );

            assertNotNull(consumeId);
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }

        try {
            consumeId = grid(0).events().remoteListen(
                new P2<UUID, DiscoveryEvent>() {
                    @Override public boolean apply(UUID uuid, DiscoveryEvent evt) {
                        return false;
                    }
                },
                new P1<DiscoveryEvent>() {
                    @Override public boolean apply(DiscoveryEvent e) {
                        return false;
                    }
                }
            );

            assertNotNull(consumeId);
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }

        try {
            consumeId = grid(0).events().remoteListen(
                new P2<UUID, Event>() {
                    @Override public boolean apply(UUID uuid, Event evt) {
                        return false;
                    }
                },
                new P1<Event>() {
                    @Override public boolean apply(Event e) {
                        return false;
                    }
                }
            );

            assertNotNull(consumeId);
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testApiAsyncOld() throws Exception {
        IgniteEvents evtAsync = grid(0).events().withAsync();

        try {
            evtAsync.stopRemoteListen(null);
            evtAsync.future().get();
        }
        catch (NullPointerException ignored) {
            // No-op.
        }

        evtAsync.stopRemoteListen(UUID.randomUUID());
        evtAsync.future().get();

        UUID consumeId = null;

        try {
            evtAsync.remoteListen(
                new P2<UUID, DiscoveryEvent>() {
                    @Override public boolean apply(UUID uuid, DiscoveryEvent evt) {
                        return false;
                    }
                },
                new P1<DiscoveryEvent>() {
                    @Override public boolean apply(DiscoveryEvent e) {
                        return false;
                    }
                },
                EVTS_DISCOVERY
            );

            consumeId = (UUID)evtAsync.future().get();

            assertNotNull(consumeId);
        }
        finally {
            evtAsync.stopRemoteListen(consumeId);
            evtAsync.future().get();
        }

        try {
            evtAsync.remoteListen(
                new P2<UUID, DiscoveryEvent>() {
                    @Override public boolean apply(UUID uuid, DiscoveryEvent evt) {
                        return false;
                    }
                },
                new P1<DiscoveryEvent>() {
                    @Override public boolean apply(DiscoveryEvent e) {
                        return false;
                    }
                }
            );

            consumeId = (UUID)evtAsync.future().get();

            assertNotNull(consumeId);
        }
        finally {
            evtAsync.stopRemoteListen(consumeId);
            evtAsync.future().get();
        }

        try {
            evtAsync.remoteListen(
                new P2<UUID, Event>() {
                    @Override public boolean apply(UUID uuid, Event evt) {
                        return false;
                    }
                },
                new P1<Event>() {
                    @Override public boolean apply(Event e) {
                        return false;
                    }
                }
            );

            consumeId = (UUID)evtAsync.future().get();

            assertNotNull(consumeId);
        }
        finally {
            evtAsync.stopRemoteListen(consumeId);
            evtAsync.future().get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testApiAsync() throws Exception {
        IgniteEvents evt = grid(0).events();

        try {
            evt.stopRemoteListenAsync(null).get();
        }
        catch (NullPointerException ignored) {
            // No-op.
        }

        evt.stopRemoteListenAsync(UUID.randomUUID()).get();

        UUID consumeId = null;

        try {
            consumeId = evt.remoteListenAsync(
                new P2<UUID, DiscoveryEvent>() {
                    @Override public boolean apply(UUID uuid, DiscoveryEvent evt) {
                        return false;
                    }
                },
                new P1<DiscoveryEvent>() {
                    @Override public boolean apply(DiscoveryEvent e) {
                        return false;
                    }
                },
                EVTS_DISCOVERY
            ).get();

            assertNotNull(consumeId);
        }
        finally {
            evt.stopRemoteListenAsync(consumeId).get();
        }

        try {
            consumeId = evt.remoteListenAsync(
                new P2<UUID, DiscoveryEvent>() {
                    @Override public boolean apply(UUID uuid, DiscoveryEvent evt) {
                        return false;
                    }
                },
                new P1<DiscoveryEvent>() {
                    @Override public boolean apply(DiscoveryEvent e) {
                        return false;
                    }
                }
            ).get();

            assertNotNull(consumeId);
        }
        finally {
            evt.stopRemoteListenAsync(consumeId).get();
        }

        try {
            consumeId = evt.remoteListenAsync(
                new P2<UUID, Event>() {
                    @Override public boolean apply(UUID uuid, Event evt) {
                        return false;
                    }
                },
                new P1<Event>() {
                    @Override public boolean apply(Event e) {
                        return false;
                    }
                }
            ).get();

            assertNotNull(consumeId);
        }
        finally {
            evt.stopRemoteListenAsync(consumeId).get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAllEvents() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT);

        UUID consumeId = grid(0).events().remoteListen(
            new P2<UUID, Event>() {
                @Override public boolean apply(UUID nodeId, Event evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    if (evt.type() == EVT_JOB_STARTED) {
                        nodeIds.add(nodeId);
                        cnt.incrementAndGet();
                        latch.countDown();
                    }

                    return true;
                }
            },
            null
        );

        try {
            assertNotNull(consumeId);

            grid(0).compute().broadcast(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(GRID_CNT, nodeIds.size());
            assertEquals(GRID_CNT, cnt.get());
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEventsByType() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT);

        UUID consumeId = grid(0).events().remoteListen(
            new P2<UUID, Event>() {
                @Override public boolean apply(UUID nodeId, Event evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());

                    nodeIds.add(nodeId);
                    cnt.incrementAndGet();
                    latch.countDown();

                    return true;
                }
            },
            null,
            EVT_JOB_STARTED
        );

        try {
            assertNotNull(consumeId);

            grid(0).compute().broadcast(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(GRID_CNT, nodeIds.size());
            assertEquals(GRID_CNT, cnt.get());
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEventsByFilter() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT);

        UUID consumeId = grid(0).events().remoteListen(
            new P2<UUID, Event>() {
                @Override public boolean apply(UUID nodeId, Event evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());

                    nodeIds.add(nodeId);
                    cnt.incrementAndGet();
                    latch.countDown();

                    return true;
                }
            },
            new P1<Event>() {
                @Override public boolean apply(Event evt) {
                    return evt.type() == EVT_JOB_STARTED;
                }
            }
        );

        try {
            assertNotNull(consumeId);

            grid(0).compute().broadcast(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(GRID_CNT, nodeIds.size());
            assertEquals(GRID_CNT, cnt.get());
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEventsByTypeAndFilter() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT);

        UUID consumeId = grid(0).events().remoteListen(
            new P2<UUID, JobEvent>() {
                @Override public boolean apply(UUID nodeId, JobEvent evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());

                    nodeIds.add(nodeId);
                    cnt.incrementAndGet();
                    latch.countDown();

                    return true;
                }
            },
            new P1<JobEvent>() {
                @Override public boolean apply(JobEvent evt) {
                    return !"exclude".equals(evt.taskName());
                }
            },
            EVT_JOB_STARTED
        );

        try {
            assertNotNull(consumeId);

            grid(0).compute().broadcast(F.noop());
            grid(0).compute().withName("exclude").run(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(GRID_CNT, nodeIds.size());
            assertEquals(GRID_CNT, cnt.get());
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoteProjection() throws Exception {
        final Collection<UUID> nodeIds = new ConcurrentSkipListSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT - 1);

        UUID consumeId = events(grid(0).cluster().forRemotes()).remoteListen(
            new P2<UUID, Event>() {
                @Override public boolean apply(UUID nodeId, Event evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());

                    nodeIds.add(nodeId);
                    cnt.incrementAndGet();
                    latch.countDown();

                    return true;
                }
            },
            null,
            EVT_JOB_STARTED
        );

        try {
            assertNotNull(consumeId);

            grid(0).compute().broadcast(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(GRID_CNT - 1, nodeIds.size());
            assertEquals(GRID_CNT - 1, cnt.get());
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProjectionWithLocalNode() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT - 1);

        UUID consumeId = events(grid(0).cluster().forAttribute("include", null)).remoteListen(
            new P2<UUID, Event>() {
                @Override public boolean apply(UUID nodeId, Event evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());

                    nodeIds.add(nodeId);
                    cnt.incrementAndGet();
                    latch.countDown();

                    return true;
                }
            },
            null,
            EVT_JOB_STARTED
        );

        try {
            assertNotNull(consumeId);

            grid(0).compute().broadcast(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(GRID_CNT - 1, nodeIds.size());
            assertEquals(GRID_CNT - 1, cnt.get());
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalNodeOnly() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);

        UUID consumeId = events(grid(0).cluster().forLocal()).remoteListen(
            new P2<UUID, Event>() {
                @Override public boolean apply(UUID nodeId, Event evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());

                    nodeIds.add(nodeId);
                    cnt.incrementAndGet();
                    latch.countDown();

                    return true;
                }
            },
            null,
            EVT_JOB_STARTED
        );

        try {
            assertNotNull(consumeId);

            grid(0).compute().broadcast(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(1, nodeIds.size());
            assertEquals(1, cnt.get());

            assertEquals(grid(0).localNode().id(), F.first(nodeIds));
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopByCallback() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);

        UUID consumeId = grid(0).events().remoteListen(
            new P2<UUID, Event>() {
                @Override public boolean apply(UUID nodeId, Event evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());

                    nodeIds.add(nodeId);
                    cnt.incrementAndGet();
                    latch.countDown();

                    return false;
                }
            },
            null,
            EVT_JOB_STARTED
        );

        try {
            assertNotNull(consumeId);

            grid(0).compute().broadcast(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(1, nodeIds.size());
            assertEquals(1, cnt.get());
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopRemoteListen() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);

        UUID consumeId = grid(0).events().remoteListen(
            new P2<UUID, Event>() {
                @Override public boolean apply(UUID nodeId, Event evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());

                    nodeIds.add(nodeId);
                    cnt.incrementAndGet();
                    latch.countDown();

                    return true;
                }
            },
            null,
            EVT_JOB_STARTED
        );

        try {
            assertNotNull(consumeId);

            grid(0).compute().run(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(1, nodeIds.size());
            assertEquals(1, cnt.get());

            grid(0).events().stopRemoteListen(consumeId);

            grid(0).compute().run(F.noop());

            U.sleep(500);

            assertEquals(1, nodeIds.size());
            assertEquals(1, cnt.get());
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopLocalListenByCallback() throws Exception {
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);

        grid(0).events().localListen(
            new P1<Event>() {
                @Override public boolean apply(Event evt) {
                    info("Local event [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());

                    cnt.incrementAndGet();
                    latch.countDown();

                    return false;
                }
            },
            EVT_JOB_STARTED);

        compute(grid(0).cluster().forLocal()).run(F.noop());

        assert latch.await(10, SECONDS) : latch;

        assertEquals(1, cnt.get());

        compute(grid(0).cluster().forLocal()).run(F.noop());

        U.sleep(500);

        assertEquals(1, cnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeJoin() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT + 1);

        UUID consumeId = grid(0).events().remoteListen(
            notSerializableProxy(new P2<UUID, Event>() {
                @Override public boolean apply(UUID nodeId, Event evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());

                    nodeIds.add(nodeId);
                    cnt.incrementAndGet();
                    latch.countDown();

                    return true;
                }
            }),
            notSerializableProxy(new P1<Event>() {
                @Override public boolean apply(Event evt) {
                    return evt.type() == EVT_JOB_STARTED;
                }
            }),
            EVT_JOB_STARTED, EVT_JOB_FINISHED
        );

        try {
            assertNotNull(consumeId);

            include = true;

            startGrid("anotherGridNodeJoin");

            grid(0).compute().broadcast(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(GRID_CNT + 1, nodeIds.size());
            assertEquals(GRID_CNT + 1, cnt.get());
        }
        finally {
            stopGrid("anotherGridNodeJoin");

            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeJoinWithProjection() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT);

        UUID consumeId = events(grid(0).cluster().forAttribute("include", null)).remoteListen(
            new P2<UUID, Event>() {
                @Override public boolean apply(UUID nodeId, Event evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());

                    nodeIds.add(nodeId);
                    cnt.incrementAndGet();
                    latch.countDown();

                    return true;
                }
            },
            null,
            EVT_JOB_STARTED
        );

        try {
            assertNotNull(consumeId);

            include = true;

            startGrid("anotherGridNodeJoinWithProjection1");

            include = false;

            startGrid("anotherGridNodeJoinWithProjection2");

            grid(0).compute().broadcast(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(GRID_CNT, nodeIds.size());
            assertEquals(GRID_CNT, cnt.get());
        }
        finally {
            stopGrid("anotherGridNodeJoinWithProjection1");
            stopGrid("anotherGridNodeJoinWithProjection2");

            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * TODO: IGNITE-585.
     *
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-585")
    @Test
    public void testNodeJoinWithP2P() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT + 1);

        ClassLoader ldr = getExternalClassLoader();

        IgnitePredicate<ClusterNode> prjPred = (IgnitePredicate<ClusterNode>)ldr.loadClass(PRJ_PRED_CLS_NAME).newInstance();
        IgnitePredicate<Event> filter = (IgnitePredicate<Event>)ldr.loadClass(FILTER_CLS_NAME).newInstance();

        UUID consumeId = events(grid(0).cluster().forPredicate(prjPred)).remoteListen(new P2<UUID, Event>() {
            @Override public boolean apply(UUID nodeId, Event evt) {
                info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                assertEquals(EVT_JOB_STARTED, evt.type());

                nodeIds.add(nodeId);
                cnt.incrementAndGet();
                latch.countDown();

                return true;
            }
        }, filter, EVT_JOB_STARTED);

        try {
            assertNotNull(consumeId);

            startGrid("anotherGridNodeJoinWithP2P");

            grid(0).compute().broadcast(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(GRID_CNT + 1, nodeIds.size());
            assertEquals(GRID_CNT + 1, cnt.get());
        }
        finally {
            stopGrid("anotherGridNodeJoinWithP2P");

            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testResources() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT);

        UUID consumeId = grid(0).events().remoteListen(
            new P2<UUID, Event>() {
                @IgniteInstanceResource
                private Ignite grid;

                @Override public boolean apply(UUID nodeId, Event evt) {
                    info("Event from " + nodeId + " [" + evt.shortDisplay() + ']');

                    assertEquals(EVT_JOB_STARTED, evt.type());
                    assertNotNull(grid);

                    nodeIds.add(nodeId);
                    cnt.incrementAndGet();
                    latch.countDown();

                    return true;
                }
            },
            new P1<Event>() {
                @IgniteInstanceResource
                private Ignite grid;

                @Override public boolean apply(Event evt) {
                    assertNotNull(grid);

                    return true;
                }
            },
            EVT_JOB_STARTED
        );

        try {
            assertNotNull(consumeId);

            grid(0).compute().broadcast(F.noop());

            assert latch.await(10, SECONDS) : latch;

            assertEquals(GRID_CNT, nodeIds.size());
            assertEquals(GRID_CNT, cnt.get());
        }
        finally {
            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMasterNodeLeave() throws Exception {
        final CountDownLatch latch = new CountDownLatch(GRID_CNT);

        Ignite g = startGrid("anotherGridMasterNodeLeave");

        try {
            final UUID nodeId = g.cluster().localNode().id();
            for (int i = 0; i < GRID_CNT; i++) {
                grid(i).events().localListen(new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        if (nodeId.equals(((DiscoveryEvent)evt).eventNode().id()))
                            latch.countDown();

                        return true;
                    }
                }, EVT_NODE_LEFT, EVT_NODE_FAILED);
            }

            g.events().remoteListen(
                null,
                new P1<Event>() {
                    @Override public boolean apply(Event evt) {
                        return true;
                    }
                },
                EVTS_ALL
            );

        }
        finally {
            stopGrid("anotherGridMasterNodeLeave");
        }

        assert latch.await(3000, MILLISECONDS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMasterNodeLeaveNoAutoUnsubscribe() throws Exception {
        Ignite g = startGrid("anotherGridMasterNodeLeaveNoAutoUnsubscribe");

        final CountDownLatch discoLatch;

        try {
            final UUID nodeId = g.cluster().localNode().id();
            discoLatch = new CountDownLatch(GRID_CNT);

            for (int i = 0; i < GRID_CNT; i++) {
                grid(0).events().localListen(new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        if (nodeId.equals(((DiscoveryEvent) evt).eventNode().id()))
                            discoLatch.countDown();

                        return true;
                    }
                }, EVT_NODE_LEFT);
            }

            consumeLatch = new CountDownLatch(GRID_CNT * 2 + 1);
            consumeCnt = new AtomicInteger();

            noAutoUnsubscribe = true;

            g.events().remoteListen(
                1, 0, false,
                null,
                new P1<Event>() {
                    @Override public boolean apply(Event evt) {
                        consumeLatch.countDown();
                        consumeCnt.incrementAndGet();

                        return true;
                    }
                },
                EVT_JOB_STARTED
            );

            grid(0).compute().broadcast(F.noop());
        }
        finally {
            stopGrid("anotherGridMasterNodeLeaveNoAutoUnsubscribe");
        }

        discoLatch.await(3000, MILLISECONDS);

        grid(0).compute().broadcast(F.noop());

        assert consumeLatch.await(2, SECONDS);

        assertEquals(GRID_CNT * 2 + 1, consumeCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultithreadedWithNodeRestart() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();
        final BlockingQueue<IgniteBiTuple<Integer, UUID>> queue = new LinkedBlockingQueue<>();
        final Collection<UUID> started = new GridConcurrentHashSet<>();
        final Collection<UUID> stopped = new GridConcurrentHashSet<>();

        final Random rnd = new Random();

        final int consumeCnt = tcpDiscovery() ? CONSUME_CNT : CONSUME_CNT / 5;

        try {
            IgniteInternalFuture<?> starterFut = multithreadedAsync(() -> {
                for (int i = 0; i < consumeCnt; i++) {
                    int idx = rnd.nextInt(GRID_CNT);

                    try {
                        IgniteEvents evts = grid(idx).events();

                        UUID consumeId = evts.remoteListenAsync(
                            (P2<UUID, Event>)(uuid, evt) -> true,
                            null,
                            EVT_JOB_STARTED
                        ).get(30_000);

                        started.add(consumeId);

                        queue.add(F.t(idx, consumeId));
                    }
                    catch (ClusterTopologyException e) {
                        log.error("Failed during consume starter", e);
                    }

                    U.sleep(10);
                }

                return null;
            }, 6, "consume-starter");

            starterFut.listen((fut) -> stop.set(true));

            IgniteInternalFuture<?> stopperFut = multithreadedAsync(() -> {
                while (!stop.get() || !queue.isEmpty()) {
                    IgniteBiTuple<Integer, UUID> t = queue.poll(1, SECONDS);

                    if (t == null)
                        continue;

                    int idx = t.get1();
                    UUID consumeId = t.get2();

                    try {
                        IgniteEvents evts = grid(idx).events();

                        evts.stopRemoteListenAsync(consumeId).get(30_000);

                        stopped.add(consumeId);
                    }
                    catch (Exception e) {
                        log.error("Failed during consume stopper", e);

                        queue.add(t);
                    }
                }

                return null;
            }, 3, "consume-stopper");

            IgniteInternalFuture<?> nodeRestarterFut = multithreadedAsync(() -> {
                while (!stop.get()) {
                    startGrid("anotherGrid");
                    stopGrid("anotherGrid");
                }

                return null;
            }, 1, "node-restarter");

            IgniteInternalFuture<?> jobRunnerFut = multithreadedAsync(() -> {
                while (!stop.get()) {
                    int idx = rnd.nextInt(GRID_CNT);

                    try {
                        grid(idx).compute().runAsync(F.noop()).get(30_000);
                    }
                    catch (IgniteException ignored) {
                        // Ignore all job execution related errors.
                    }
                }

                return null;
            }, 1, "job-runner");

            GridTestUtils.waitForAllFutures(starterFut, stopperFut, nodeRestarterFut, jobRunnerFut);

            Collection<UUID> notStopped = F.lose(started, true, stopped);

            assertEquals("Not stopped IDs: " + notStopped, 0, notStopped.size());
        }
        finally {
            stop.set(true);

            queue.clear();
        }
    }
}
