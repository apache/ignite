/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.gridgain.grid.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.streamer.window.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Tests for streamer eviction logic.
 */
public class GridStreamerEvictionSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private boolean atLeastOnce = true;

    /** Test stages. */
    private Collection<GridStreamerStage> stages;

    /** Event router. */
    private GridStreamerEventRouter router;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setStreamerConfiguration(streamerConfiguration());

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        return cfg;
    }

    /**
     * @return Streamer configuration.
     */
    private GridStreamerConfiguration streamerConfiguration() {
        GridStreamerConfiguration cfg = new GridStreamerConfiguration();

        cfg.setAtLeastOnce(atLeastOnce);

        cfg.setRouter(router);

        GridStreamerBoundedTimeWindow window = new GridStreamerBoundedTimeWindow();
        window.setName("window1");
        window.setTimeInterval(5 * 60000);

        cfg.setWindows(F.asList((GridStreamerWindow)window));

        cfg.setStages(stages);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testContextNextStage() throws Exception {
        atLeastOnce = true;
        router = new GridTestStreamerEventRouter();

        final CountDownLatch finishLatch = new CountDownLatch(100);
        final AtomicReference<GridException> err = new AtomicReference<>();

        SC stage = new SC() {
            @SuppressWarnings("unchecked")
            @Override public Map<String, Collection<?>> applyx(String stageName, GridStreamerContext ctx,
                Collection<Object> evts) throws GridException {
                GridStreamerWindow win = ctx.window("window1");
                String nextStage = ctx.nextStageName();

                if (nextStage == null) {
                    finishLatch.countDown();

                    return null;
                }

                assert evts.size() == 1;

                // Add new events to the window.
                win.enqueueAll(evts);

                // Evict outdated events from the window.
                Collection c = win.pollEvictedAll();
                System.out.println(" --> evicted: " + c.size());


                Integer val = (Integer)F.first(evts);

                val++;

                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    e.printStackTrace(); // TODO implement.
                }


                return (Map)F.asMap(ctx.nextStageName(), F.asList(val));
            }
        };

        stages = F.asList(
            (GridStreamerStage)new GridTestStage("0", stage),
            new GridTestStage("1", stage),
            new GridTestStage("2", stage),
            new GridTestStage("3", stage),
            new GridTestStage("4", stage)
        );

        startGrids(4);

        try {
            GridTestStreamerEventRouter router0 = (GridTestStreamerEventRouter)router;

            router0.put("0", grid(1).localNode().id());
            router0.put("1", grid(2).localNode().id());
            router0.put("2", grid(3).localNode().id());
            router0.put("3", grid(0).localNode().id());
            router0.put("4", grid(1).localNode().id());

            for (int i = 0; i < 100; i++) {
                grid(0).streamer(null).addEvent(i);

                Collection<GridStreamerWindow> win = grid(0).streamer(null).configuration().getWindows();

                System.out.println(" --> " + win.iterator().next().evictionQueueSize());
            }

            finishLatch.await();


            if (err.get() != null)
                throw err.get();
        }
        finally {
            stopAllGrids(false);
        }
    }
}
