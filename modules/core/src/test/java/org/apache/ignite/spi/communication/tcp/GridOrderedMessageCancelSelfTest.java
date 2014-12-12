/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 *
 */
public class GridOrderedMessageCancelSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cancel latch. */
    private static CountDownLatch cancelLatch;

    /** Process response latch. */
    private static CountDownLatch resLatch;

    /** Finish latch. */
    private static CountDownLatch finishLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setPreloadMode(NONE);

        cfg.setCacheConfiguration(cache);

        cfg.setCommunicationSpi(new CommunicationSpi());

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cancelLatch = new CountDownLatch(1);
        resLatch = new CountDownLatch(1);
        finishLatch = new CountDownLatch(1);

        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQuery() throws Exception {
        GridCacheQueryFuture<Map.Entry<Object, Object>> fut =
            grid(0).cache(null).queries().createSqlQuery(String.class, "_key is not null").execute();

        testMessageSet(fut);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTask() throws Exception {
        ComputeTaskFuture<?> fut = executeAsync(compute(grid(0).forRemotes()), Task.class, null);

        testMessageSet(fut);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskException() throws Exception {
        ComputeTaskFuture<?> fut = executeAsync(compute(grid(0).forRemotes()), FailTask.class, null);

        testMessageSet(fut);
    }

    /**
     * @param fut Future to cancel.
     * @throws Exception If failed.
     */
    private void testMessageSet(IgniteFuture<?> fut) throws Exception {
        cancelLatch.await();

        assertTrue(fut.cancel());

        resLatch.countDown();

        finishLatch.await();

        Map map = U.field(((GridKernal)grid(0)).context().io(), "msgSetMap");

        info("Map: " + map);

        assertTrue(map.isEmpty());
    }

    /**
     * Communication SPI.
     */
    private static class CommunicationSpi extends TcpCommunicationSpi {
        /** */
        @IgniteMarshallerResource
        private IgniteMarshaller marsh;

        /** {@inheritDoc} */
        @Override protected void notifyListener(UUID sndId, GridTcpCommunicationMessageAdapter msg,
            IgniteRunnable msgC) {
            GridIoMessage ioMsg = (GridIoMessage)msg;

            boolean wait = ioMsg.message() instanceof GridCacheQueryResponse ||
                ioMsg.message() instanceof GridJobExecuteResponse;

            if (wait) {
                cancelLatch.countDown();

                U.awaitQuiet(resLatch);
            }

            super.notifyListener(sndId, msg, msgC);

            if (wait)
                finishLatch.countDown();
        }
    }

    /**
     * Test task.
     */
    @ComputeTaskSessionFullSupport
    private static class Task extends ComputeTaskSplitAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Void arg) throws IgniteCheckedException {
            return Collections.singleton(new ComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return null;
        }
    }

    /**
     * Test task.
     */
    @ComputeTaskSessionFullSupport
    private static class FailTask extends ComputeTaskSplitAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Void arg) throws IgniteCheckedException {
            return Collections.singleton(new ComputeJobAdapter() {
                @Nullable @Override public Object execute() throws IgniteCheckedException {
                    throw new IgniteCheckedException("Task failed.");
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return null;
        }
    }
}
