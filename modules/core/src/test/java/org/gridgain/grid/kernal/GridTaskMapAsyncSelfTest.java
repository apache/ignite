/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 *
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskMapAsyncSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridTaskMapAsyncSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskMap() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        info("Executing sync mapped task.");

        ignite.compute().execute(SyncMappedTask.class, null);

        info("Executing async mapped task.");

        ignite.compute().execute(AsyncMappedTask.class, null);
    }

    /**
     *
     */
    @GridComputeTaskMapAsync
    private static class AsyncMappedTask extends BaseTask {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            Collection<? extends GridComputeJob> res = super.split(gridSize, arg);

            assert mainThread != mapper;

            return res;
        }
    }

    /**
     *
     */
    private static class SyncMappedTask extends BaseTask {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            Collection<? extends GridComputeJob> res = super.split(gridSize, arg);

            assert mainThread == mapper;

            return res;
        }
    }

    /**
     * Test task.
     */
    private abstract static class BaseTask extends GridComputeTaskSplitAdapter<Object, Void> {
        /** */
        protected static final Thread mainThread = Thread.currentThread();

        /** */
        protected Thread mapper;

        /** */
        protected Thread runner;

        /** */
        @GridLoggerResource
        protected GridLogger log;

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            mapper = Thread.currentThread();

            return Collections.singleton(new GridComputeJobAdapter() {
                @Override public Serializable execute() {
                    runner = Thread.currentThread();

                    log.info("Runner: " + runner);
                    log.info("Main: " + mainThread);
                    log.info("Mapper: " + mapper);

                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }
}
