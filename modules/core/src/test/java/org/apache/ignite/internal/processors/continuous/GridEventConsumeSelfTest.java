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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.*;

/**
 * Event consume test.
 */
public class GridEventConsumeSelfTest extends GridCommonAbstractTest implements Serializable {
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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (include)
            cfg.setUserAttributes(F.asMap("include", true));

        cfg.setMarshaller(new OptimizedMarshaller(false));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        assertTrue(GRID_CNT > 1);

        include = true;

        startGridsMultiThreaded(GRID_CNT - 1);

        include = false;

        startGrid(GRID_CNT - 1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        assertEquals(GRID_CNT, grid(0).cluster().nodes().size());

        for (int i = 0; i < GRID_CNT; i++) {
            IgniteKernal grid = (IgniteKernal)grid(i);

            GridContinuousProcessor proc = grid.context().continuous();

            if (noAutoUnsubscribe) {
                localRoutines(proc).clear();

                U.<Map>field(proc, "rmtInfos").clear();
            }

            assertEquals(0, localRoutines(proc).size());
            assertEquals(0, U.<Map>field(proc, "rmtInfos").size());
            assertEquals(0, U.<Map>field(proc, "startFuts").size());
            assertEquals(0, U.<Map>field(proc, "waitForStartAck").size());
            assertEquals(0, U.<Map>field(proc, "stopFuts").size());
            assertEquals(0, U.<Map>field(proc, "waitForStopAck").size());
            assertEquals(0, U.<Map>field(proc, "pending").size());
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
                    return info.handler().isForEvents();
                }
            });
    }

    /**
     * @throws Exception If failed.
     */
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

            assert latch.await(2, SECONDS);

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

            assert latch.await(2, SECONDS);

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

            assert latch.await(2, SECONDS);

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

            assert latch.await(2, SECONDS);

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
    public void testRemoteProjection() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
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

            assert latch.await(2, SECONDS);

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

            assert latch.await(2, SECONDS);

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

            assert latch.await(2, SECONDS);

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
    public void testEmptyProjection() throws Exception {
        try {
            events(grid(0).cluster().forPredicate(F.<ClusterNode>alwaysFalse())).remoteListen(
                new P2<UUID, Event>() {
                    @Override public boolean apply(UUID nodeId, Event evt) {
                        return true;
                    }
                },
                null
            );

            assert false : "Exception was not thrown.";
        }
        catch (IgniteException e) {
            assertTrue(e.getMessage().startsWith(
                "Failed to register remote continuous listener (projection is empty)."));
        }
    }

    /**
     * @throws Exception If failed.
     */
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

            assert latch.await(2, SECONDS);

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

            assert latch.await(2, SECONDS);

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

        assert latch.await(2, SECONDS);

        assertEquals(1, cnt.get());

        compute(grid(0).cluster().forLocal()).run(F.noop());

        U.sleep(500);

        assertEquals(1, cnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeJoin() throws Exception {
        final Collection<UUID> nodeIds = new HashSet<>();
        final AtomicInteger cnt = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT + 1);

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
            },
            EVT_JOB_STARTED, EVT_JOB_FINISHED
        );

        try {
            assertNotNull(consumeId);

            include = true;

            startGrid("anotherGrid");

            grid(0).compute().broadcast(F.noop());

            assert latch.await(2, SECONDS);

            assertEquals(GRID_CNT + 1, nodeIds.size());
            assertEquals(GRID_CNT + 1, cnt.get());
        }
        finally {
            stopGrid("anotherGrid");

            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
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

            startGrid("anotherGrid1");

            include = false;

            startGrid("anotherGrid2");

            grid(0).compute().broadcast(F.noop());

            assert latch.await(2, SECONDS);

            assertEquals(GRID_CNT, nodeIds.size());
            assertEquals(GRID_CNT, cnt.get());
        }
        finally {
            stopGrid("anotherGrid1");
            stopGrid("anotherGrid2");

            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * TODO: IGNITE-585.
     *
     * @throws Exception If failed.
     */
    public void testNodeJoinWithP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-585");
        
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

            startGrid("anotherGrid");

            grid(0).compute().broadcast(F.noop());

            assert latch.await(2, SECONDS);

            assertEquals(GRID_CNT + 1, nodeIds.size());
            assertEquals(GRID_CNT + 1, cnt.get());
        }
        finally {
            stopGrid("anotherGrid1");
            stopGrid("anotherGrid2");

            grid(0).events().stopRemoteListen(consumeId);
        }
    }

    /**
     * @throws Exception If failed.
     */
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

            assert latch.await(2, SECONDS);

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
    public void testMasterNodeLeave() throws Exception {
        Ignite g = startGrid("anotherGrid");

        final UUID nodeId = g.cluster().localNode().id();
        final CountDownLatch latch = new CountDownLatch(GRID_CNT);

        for (int i = 0; i < GRID_CNT; i++) {
            grid(0).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    if (nodeId.equals(((DiscoveryEvent) evt).eventNode().id()))
                        latch.countDown();

                    return true;
                }
            }, EVT_NODE_LEFT);
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

        stopGrid("anotherGrid");

        latch.await();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMasterNodeLeaveNoAutoUnsubscribe() throws Exception {
        Ignite g = startGrid("anotherGrid");

        final UUID nodeId = g.cluster().localNode().id();
        final CountDownLatch discoLatch = new CountDownLatch(GRID_CNT);

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

        stopGrid("anotherGrid");

        discoLatch.await();

        grid(0).compute().broadcast(F.noop());

        assert consumeLatch.await(2, SECONDS);

        assertEquals(GRID_CNT * 2 + 1, consumeCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedWithNodeRestart() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();
        final BlockingQueue<IgniteBiTuple<Integer, UUID>> queue = new LinkedBlockingQueue<>();
        final Collection<UUID> started = new GridConcurrentHashSet<>();
        final Collection<UUID> stopped = new GridConcurrentHashSet<>();

        final Random rnd = new Random();

        IgniteInternalFuture<?> starterFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < CONSUME_CNT; i++) {
                    int idx = rnd.nextInt(GRID_CNT);

                    try {
                        IgniteEvents evts = grid(idx).events().withAsync();

                        evts.remoteListen(new P2<UUID, Event>() {
                            @Override public boolean apply(UUID uuid, Event evt) {
                                return true;
                            }
                        }, null, EVT_JOB_STARTED);

                        UUID consumeId = evts.<UUID>future().get(3000);

                        started.add(consumeId);

                        queue.add(F.t(idx, consumeId));
                    }
                    catch (ClusterTopologyException ignored) {
                        // No-op.
                    }

                    U.sleep(10);
                }

                stop.set(true);

                return null;
            }
        }, 8, "consume-starter");

        IgniteInternalFuture<?> stopperFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!stop.get()) {
                    IgniteBiTuple<Integer, UUID> t = queue.poll(1, SECONDS);

                    if (t == null)
                        continue;

                    int idx = t.get1();
                    UUID consumeId = t.get2();

                    try {
                        IgniteEvents evts = grid(idx).events().withAsync();

                        evts.stopRemoteListen(consumeId);

                        evts.future().get(3000);

                        stopped.add(consumeId);
                    }
                    catch (ClusterTopologyException ignored) {
                        // No-op.
                    }
                }

                return null;
            }
        }, 4, "consume-stopper");

        IgniteInternalFuture<?> nodeRestarterFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!stop.get()) {
                    startGrid("anotherGrid");
                    stopGrid("anotherGrid");
                }

                return null;
            }
        }, 1, "node-restarter");

        IgniteInternalFuture<?> jobRunnerFut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!stop.get()) {
                    int idx = rnd.nextInt(GRID_CNT);

                    try {
                        IgniteCompute comp = grid(idx).compute().withAsync();

                        comp.run(F.noop());

                        comp.future().get(3000);
                    }
                    catch (IgniteException ignored) {
                        // Ignore all job execution related errors.
                    }
                }

                return null;
            }
        }, 1, "job-runner");

        starterFut.get();
        stopperFut.get();
        nodeRestarterFut.get();
        jobRunnerFut.get();

        IgniteBiTuple<Integer, UUID> t;

        while ((t = queue.poll()) != null) {
            int idx = t.get1();
            UUID consumeId = t.get2();

            IgniteEvents evts = grid(idx).events().withAsync();

            evts.stopRemoteListen(consumeId);

            evts.future().get(3000);

            stopped.add(consumeId);
        }

        Collection<UUID> notStopped = F.lose(started, true, stopped);

        assertEquals("Not stopped IDs: " + notStopped, 0, notStopped.size());
    }
}
