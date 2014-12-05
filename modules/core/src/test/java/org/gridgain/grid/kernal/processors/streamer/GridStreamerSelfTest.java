/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.streamer.*;
import org.apache.ignite.streamer.router.*;
import org.apache.ignite.streamer.window.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.configuration.GridDeploymentMode.*;

/**
 * Basic streamer test.
 */
public class GridStreamerSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private boolean atLeastOnce = true;

    /** Test stages. */
    private Collection<StreamerStage> stages;

    /** Event router. */
    private StreamerEventRouter router;

    /** P2P enabled flag. */
    private boolean p2pEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setStreamerConfiguration(streamerConfiguration());

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setPeerClassLoadingEnabled(p2pEnabled);

        if (p2pEnabled)
            cfg.setDeploymentMode(SHARED);

        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        return cfg;
    }

    /**
     * @return Streamer configuration.
     */
    private StreamerConfiguration streamerConfiguration() {
        StreamerConfiguration cfg = new StreamerConfiguration();

        cfg.setAtLeastOnce(atLeastOnce);

        cfg.setRouter(router);

        cfg.setWindows(F.asList((StreamerWindow)new StreamerUnboundedWindow()));

        cfg.setStages(stages);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testInjections() throws Exception {
        final int evtCnt = 100;

        final CountDownLatch finishLatch = new CountDownLatch(evtCnt);

        stages = F.<StreamerStage>asList(new StreamerStage() {
            @IgniteInstanceResource
            private Ignite g;

            @IgniteLoggerResource
            private IgniteLogger log;

            @Override public String name() {
                return "name";
            }

            @Nullable @Override public Map<String, Collection<?>> run(StreamerContext ctx, Collection evts) {
                assert g != null;
                assert log != null;

                log.info("Processing events: " + evts);

                finishLatch.countDown();

                return null;
            }
        });

        try {
            final Ignite ignite0 = startGrid(0);

            IgniteStreamer streamer = ignite0.streamer(null);

            for (int i = 0; i < evtCnt; i++)
                streamer.addEvent("event1");

            assert finishLatch.await(10, SECONDS);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStreamerMetrics() throws Exception {
        atLeastOnce = true;
        p2pEnabled = false;
        router = new GridTestStreamerEventRouter();

        final int evtCnt = 100;

        final CountDownLatch finishLatch = new CountDownLatch(evtCnt);

        SC stage = new SC() {
            @SuppressWarnings("unchecked")
            @Override public Map<String, Collection<?>> applyx(String stageName, StreamerContext ctx,
                Collection<Object> evts)
                throws GridException {
                String nextStage = ctx.nextStageName();

                U.sleep(50);

                if (nextStage == null) {
                    finishLatch.countDown();

                    return null;
                }

                return (Map)F.asMap(nextStage, evts);
            }
        };

        stages = F.asList((StreamerStage)new GridTestStage("a", stage), new GridTestStage("b", stage),
            new GridTestStage("c", stage));

        startGrids(4);

        try {
            final Ignite ignite0 = grid(0);
            final Ignite ignite1 = grid(1);
            final Ignite ignite2 = grid(2);
            final Ignite ignite3 = grid(3);

            System.out.println("Grid 0: " + ignite0.cluster().localNode().id());
            System.out.println("Grid 1: " + ignite1.cluster().localNode().id());
            System.out.println("Grid 2: " + ignite2.cluster().localNode().id());
            System.out.println("Grid 3: " + ignite3.cluster().localNode().id());

            GridTestStreamerEventRouter router0 = (GridTestStreamerEventRouter)router;

            router0.put("a", ignite1.cluster().localNode().id());
            router0.put("b", ignite2.cluster().localNode().id());
            router0.put("c", ignite3.cluster().localNode().id());

            IgniteStreamer streamer = ignite0.streamer(null);

            for (int i = 0; i < evtCnt; i++)
                streamer.addEvent("event1");

            finishLatch.await();

            // No stages should be executed on grid0.
            checkZeroMetrics(ignite0, "a", "b", "c");
            checkZeroMetrics(ignite1, "b", "c");
            checkZeroMetrics(ignite2, "a", "c");
            checkZeroMetrics(ignite3, "a", "b");

            checkMetrics(ignite1, "a", evtCnt, false);
            checkMetrics(ignite2, "b", evtCnt, false);
            checkMetrics(ignite3, "c", evtCnt, true);

            // Wait until all acks are received.
            GridTestUtils.retryAssert(log, 100, 50, new CA() {
                @Override public void apply() {
                    StreamerMetrics metrics = ignite0.streamer(null).metrics();

                    assertEquals(0, metrics.currentActiveSessions());
                }
            });

            StreamerMetrics metrics = ignite0.streamer(null).metrics();

            assertTrue(metrics.maximumActiveSessions() > 0);

            ignite0.streamer(null).context().query(new IgniteClosure<StreamerContext, Object>() {
                @Override public Object apply(StreamerContext ctx) {
                    try {
                        U.sleep(1000);
                    }
                    catch (GridInterruptedException ignore) {
                        // No-op.
                    }

                    return null;
                }
            });

            metrics = ignite0.streamer(null).metrics();

            assert metrics.queryMaximumExecutionNodes() == 4;
            assert metrics.queryMinimumExecutionNodes() == 4;
            assert metrics.queryAverageExecutionNodes() == 4;

            assert metrics.queryMaximumExecutionTime() > 0;
            assert metrics.queryMinimumExecutionTime() > 0;
            assert metrics.queryAverageExecutionTime() > 0;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testContextNextStage() throws Exception {
        atLeastOnce = true;
        router = new GridTestStreamerEventRouter();
        p2pEnabled = false;

        final CountDownLatch finishLatch = new CountDownLatch(1);
        final AtomicReference<GridException> err = new AtomicReference<>();

        SC stage = new SC() {
            @SuppressWarnings("unchecked")
            @Override public Map<String, Collection<?>> applyx(String stageName, StreamerContext ctx,
                Collection<Object> evts) {
                String nextStage = ctx.nextStageName();

                if (nextStage == null) {
                    finishLatch.countDown();

                    return null;
                }

                assert evts.size() == 1;

                Integer val = (Integer)F.first(evts);

                val++;

                if (!String.valueOf(val).equals(ctx.nextStageName()))
                    err.compareAndSet(null, new GridException("Stage name comparison failed [exp=" + val +
                        ", actual=" + ctx.nextStageName() + ']'));

                return (Map)F.asMap(ctx.nextStageName(), F.asList(val));
            }
        };

        stages = F.asList((StreamerStage)new GridTestStage("0", stage), new GridTestStage("1", stage),
            new GridTestStage("2", stage), new GridTestStage("3", stage), new GridTestStage("4", stage));

        startGrids(4);

        try {
            GridTestStreamerEventRouter router0 = (GridTestStreamerEventRouter)router;

            router0.put("0", grid(1).localNode().id());
            router0.put("1", grid(2).localNode().id());
            router0.put("2", grid(3).localNode().id());
            router0.put("3", grid(0).localNode().id());
            router0.put("4", grid(1).localNode().id());

            grid(0).streamer(null).addEvent(0);

            finishLatch.await();

            if (err.get() != null)
                throw err.get();
        }
        finally {
            stopAllGrids(false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddEventWithNullStageName() throws Exception {
        atLeastOnce = true;
        router = new GridTestStreamerEventRouter();
        p2pEnabled = false;

        SC stage = new SC() {
            @SuppressWarnings("unchecked")
            @Override public Map<String, Collection<?>> applyx(String stageName, StreamerContext ctx,
                Collection<Object> evts) {
                String nextStage = ctx.nextStageName();

                if (nextStage == null)
                    return null;

                Integer val = (Integer)F.first(evts);

                return (Map)F.asMap(ctx.nextStageName(), F.asList(++val));
            }
        };

        stages = F.asList((StreamerStage)new GridTestStage("0", stage), new GridTestStage("1", stage));

        startGrids(2);

        try {
            GridTestStreamerEventRouter router0 = (GridTestStreamerEventRouter)router;

            router0.put("0", grid(0).localNode().id());
            router0.put("1", grid(1).localNode().id());

            try {
                grid(0).streamer(null).addEventToStage(null, 0);

                fail();
            }
            catch (NullPointerException e) {
                assertTrue(e.getMessage().contains("Argument cannot be null: stageName"));

                info("Caught expected exception: " + e.getMessage());
            }

            try {
                grid(0).streamer(null).addEventsToStage(null, Collections.singletonList(0));

                fail();
            }
            catch (NullPointerException e) {
                assertTrue(e.getMessage().contains("Argument cannot be null: stageName"));

                info("Caught expected exception: " + e.getMessage());
            }
        }
        finally {
            stopAllGrids(false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullStageNameInResultMap() throws Exception {
        atLeastOnce = true;
        router = new GridTestStreamerEventRouter();
        p2pEnabled = false;

        SC stage = new SC() {
            @SuppressWarnings("unchecked")
            @Override public Map<String, Collection<?>> applyx(String stageName, StreamerContext ctx,
                Collection<Object> evts) {
                String nextStage = ctx.nextStageName();

                if (nextStage == null)
                    return null;

                Integer val = (Integer)F.first(evts);

                Map<String, Collection<?>> res = new HashMap<>();

                res.put(null, F.asList(++val));

                return res;
            }
        };

        stages = F.asList((StreamerStage)new GridTestStage("0", stage), new GridTestStage("1", stage));

        startGrids(2);

        try {
            GridTestStreamerEventRouter router0 = (GridTestStreamerEventRouter)router;

            final CountDownLatch errLatch = new CountDownLatch(1);

            grid(0).streamer(null).addStreamerFailureListener(new StreamerFailureListener() {
                @Override public void onFailure(String stageName, Collection<Object> evts, Throwable err) {
                    info("Expected failure: " + err.getMessage());

                    errLatch.countDown();
                }
            });

            router0.put("0", grid(0).localNode().id());
            router0.put("1", grid(1).localNode().id());

            grid(0).streamer(null).addEvent(0);

            assert errLatch.await(5, TimeUnit.SECONDS);
        }
        finally {
            stopAllGrids(false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPeerDeployment() throws Exception {
        URL[] urls = new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};

        GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(urls);

        Class<?> cls = ldr.loadClass("org.gridgain.grid.tests.p2p.GridCacheDeploymentTestKey");

        assert cls != null;

        final int evtCnt = 100;

        final CountDownLatch finishLatch = new CountDownLatch(evtCnt);

        SC stage = new SC() {
            @SuppressWarnings("unchecked")
            @Override public Map<String, Collection<?>> applyx(String stageName, StreamerContext ctx,
                Collection<Object> evts) throws GridException {
                String nextStage = ctx.nextStageName();

                ctx.window().enqueueAll(evts);

                if (nextStage == null) {
                    finishLatch.countDown();

                    return null;
                }

                return (Map)F.asMap(nextStage, evts);
            }
        };

        stages = F.asList((StreamerStage)new GridTestStage("a", stage), new GridTestStage("b", stage),
            new GridTestStage("c", stage));
        router = new GridTestStreamerEventRouter();
        atLeastOnce = true;
        p2pEnabled = true;

        startGrids(4);

        try {
            final Ignite ignite0 = grid(0);
            final Ignite ignite1 = grid(1);
            final Ignite ignite2 = grid(2);
            final Ignite ignite3 = grid(3);

            System.out.println("Grid 0: " + ignite0.cluster().localNode().id());
            System.out.println("Grid 1: " + ignite1.cluster().localNode().id());
            System.out.println("Grid 2: " + ignite2.cluster().localNode().id());
            System.out.println("Grid 3: " + ignite3.cluster().localNode().id());

            GridTestStreamerEventRouter router0 = (GridTestStreamerEventRouter)router;

            router0.put("a", ignite1.cluster().localNode().id());
            router0.put("b", ignite2.cluster().localNode().id());
            router0.put("c", ignite3.cluster().localNode().id());

            IgniteStreamer streamer = ignite0.streamer(null);

            for (int i = 0; i < evtCnt; i++)
                streamer.addEvent(cls.newInstance());

            // Wait for all events to be processed.
            finishLatch.await();

            for (int i = 1; i < 4; i++)
                assertEquals(evtCnt, grid(i).streamer(null).context().window().size());

            // Check undeploy.
            stopGrid(0, false);

            GridTestUtils.retryAssert(log, 50, 50, new CA() {
                @Override public void apply() {
                    for (int i = 1; i < 4; i++)
                        assertEquals(0, grid(i).streamer(null).context().window().size());
                }
            });
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testQuery() throws Exception {
        atLeastOnce = true;
        router = new StreamerRandomEventRouter();
        p2pEnabled = false;

        final int evtCnt = 1000;

        final CountDownLatch finishLatch = new CountDownLatch(evtCnt);

        SC stage = new SC() {
            @SuppressWarnings("unchecked")
            @Override public Map<String, Collection<?>> applyx(String stageName, StreamerContext ctx,
                Collection<Object> evts) {
                ConcurrentMap<String, AtomicInteger> space = ctx.localSpace();

                AtomicInteger cntr = space.get(stageName);

                if (cntr == null)
                    cntr = F.addIfAbsent(space, stageName, new AtomicInteger());

                for (Object val : evts)
                    cntr.addAndGet((Integer)val);

                String next = ctx.nextStageName();

                if (next == null) {
                    finishLatch.countDown();

                    return null;
                }

                return (Map)F.asMap(next, evts);
            }
        };

        stages = F.asList((StreamerStage)new GridTestStage("a", stage), new GridTestStage("b", stage),
            new GridTestStage("c", stage), new GridTestStage("d", stage));

        startGrids(4);

        try {
            int sum = 0;

            int range = 1000;

            Random rnd = new Random();

            for (int i = 0; i < evtCnt; i++) {
                int val = rnd.nextInt(range);

                grid(0).streamer(null).addEvent(val);

                sum += val;
            }

            finishLatch.await();

            Map<String, Integer> stagesSum = new HashMap<>(4);

            final String[] stages = {"a", "b", "c", "d"};

            // Check all stages local map.
            for (int i = 0; i < 4; i++) {
                Ignite ignite = grid(i);

                ConcurrentMap<String, AtomicInteger> locSpace = ignite.streamer(null).context().localSpace();

                for (String stageName : stages) {
                    AtomicInteger val = locSpace.get(stageName);

                    assertNotNull(val);

                    info(">>>>> grid=" + ignite.cluster().localNode().id() + ", s=" + stageName + ", val=" + val.get());

                    Integer old = stagesSum.get(stageName);

                    if (old == null)
                        stagesSum.put(stageName, val.get());
                    else
                        stagesSum.put(stageName, old + val.get());
                }
            }

            for (String s : stages)
                assertEquals((Integer)sum, stagesSum.get(s));

            StreamerContext streamerCtx = grid(0).streamer(null).context();

            // Check query.
            for (final String s : stages) {
                Collection<Integer> res = streamerCtx.query(new C1<StreamerContext, Integer>() {
                    @Override public Integer apply(StreamerContext ctx) {
                        AtomicInteger cntr = ctx.<String, AtomicInteger>localSpace().get(s);

                        return cntr.get();
                    }
                });

                assertEquals(sum, F.sumInt(res));
            }

            // Check broadcast.
            streamerCtx.broadcast(new CI1<StreamerContext>() {
                @Override public void apply(StreamerContext ctx) {
                    int sum = 0;

                    ConcurrentMap<String, AtomicInteger> space = ctx.localSpace();

                    for (String s : stages) {
                        AtomicInteger cntr = space.get(s);

                        sum += cntr.get();
                    }

                    space.put("bcast", new AtomicInteger(sum));
                }
            });

            int bcastSum = 0;

            for (int i = 0; i < 4; i++) {
                Ignite ignite = grid(i);

                ConcurrentMap<String, AtomicInteger> locSpace = ignite.streamer(null).context().localSpace();

                bcastSum += locSpace.get("bcast").get();
            }

            assertEquals(sum * stages.length, bcastSum);

            // Check reduce.
            for (final String s : stages) {
                Integer res = streamerCtx.reduce(
                    new C1<StreamerContext, Integer>() {
                        @Override public Integer apply(StreamerContext ctx) {
                            AtomicInteger cntr = ctx.<String, AtomicInteger>localSpace().get(s);

                            return cntr.get();
                        }
                    },
                    F.sumIntReducer());

                assertEquals((Integer)sum, res);
            }
        }
        finally {
            stopAllGrids(false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomRouterWithEmptyTopology() throws Exception {
        atLeastOnce = true;
        router = new StreamerRandomEventRouter(new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return false;
            }
        });
        p2pEnabled = false;

        SC stage = new SC() {
            @SuppressWarnings("unchecked")
            @Override public Map<String, Collection<?>> applyx(String stageName, StreamerContext ctx,
                Collection<Object> evts) {
                return ctx.nextStageName() == null ? null : (Map)F.asMap(ctx.nextStageName(), F.asList(0));
            }
        };

        stages = F.asList((StreamerStage)new GridTestStage("0", stage),new GridTestStage("1", stage),
            new GridTestStage("2", stage));

        startGrids(1);

        try {
            final int errCnt = 10;

            final CountDownLatch errLatch = new CountDownLatch(errCnt);

            grid(0).streamer(null).addStreamerFailureListener(new StreamerFailureListener() {
                @Override public void onFailure(String stageName, Collection<Object> evts, Throwable err) {
                    info("Expected failure: " + err.getMessage());

                    errLatch.countDown();
                }
            });

            for (int i = 0; i < errCnt; i++)
                grid(0).streamer(null).addEvent(0);

            assert errLatch.await(5, TimeUnit.SECONDS);
        }
        finally {
            stopAllGrids(false);
        }
    }

    /**
     * @param ignite Grid to check metrics on.
     * @param stage Stage name.
     * @param evtCnt Event count.
     * @param pipeline Pipeline.
     */
    private void checkMetrics(Ignite ignite, String stage, int evtCnt, boolean pipeline) {
        IgniteStreamer streamer = ignite.streamer(null);

        StreamerMetrics metrics = streamer.metrics();

        assertEquals(evtCnt, metrics.stageTotalExecutionCount());
        assertEquals(0, metrics.stageWaitingExecutionCount());
        assertEquals(0, metrics.currentActiveSessions());
        assertEquals(0, metrics.maximumActiveSessions());
        assertEquals(0, metrics.failuresCount());

        if (pipeline) {
            assertEquals(4, metrics.pipelineMaximumExecutionNodes());
            assertEquals(4, metrics.pipelineMinimumExecutionNodes());
            assertEquals(4, metrics.pipelineAverageExecutionNodes());

            assertTrue(metrics.pipelineMaximumExecutionTime() > 0);
            assertTrue(metrics.pipelineMinimumExecutionTime() > 0);
            assertTrue(metrics.pipelineAverageExecutionTime() > 0);
        }
        else {
            assertEquals(0, metrics.pipelineMaximumExecutionNodes());
            assertEquals(0, metrics.pipelineMinimumExecutionNodes());
            assertEquals(0, metrics.pipelineAverageExecutionNodes());

            assertEquals(0, metrics.pipelineMaximumExecutionTime());
            assertEquals(0, metrics.pipelineMinimumExecutionTime());
            assertEquals(0, metrics.pipelineAverageExecutionTime());
        }

        StreamerStageMetrics stageMetrics = streamer.metrics().stageMetrics(stage);

        assertNotNull(stageMetrics);

        assertTrue(stageMetrics.averageExecutionTime() > 0);
        assertTrue(stageMetrics.minimumExecutionTime() > 0);
        assertTrue(stageMetrics.maximumExecutionTime() > 0);
        assertEquals(evtCnt, stageMetrics.totalExecutionCount());
        assertEquals(0, stageMetrics.failuresCount());
        assertFalse(stageMetrics.executing());
    }

    /**
     * @param ignite Grid to check streamer on.
     * @param stages Stages to check.
     */
    private void checkZeroMetrics(Ignite ignite, String... stages) {
        for (String stage : stages) {
            IgniteStreamer streamer = ignite.streamer(null);

            StreamerStageMetrics metrics = streamer.metrics().stageMetrics(stage);

            assertNotNull(metrics);

            assertEquals(0, metrics.failuresCount());
            assertEquals(0, metrics.averageExecutionTime());
            assertEquals(0, metrics.minimumExecutionTime());
            assertEquals(0, metrics.maximumExecutionTime());
            assertFalse(metrics.executing());
        }
    }

}
