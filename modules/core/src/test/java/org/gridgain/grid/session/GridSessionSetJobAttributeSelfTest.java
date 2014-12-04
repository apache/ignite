/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.session;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
@SuppressWarnings({"CatchGenericClass"})
@GridCommonTest(group = "Task Session")
public class GridSessionSetJobAttributeSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int SPLIT_COUNT = 5;

    /** */
    public static final int EXEC_COUNT = 5;

    /** */
    public GridSessionSetJobAttributeSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        c.setExecutorService(
            new ThreadPoolExecutor(
                SPLIT_COUNT * EXEC_COUNT,
                SPLIT_COUNT * EXEC_COUNT,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));

        c.setExecutorServiceShutdown(true);

        return c;
    }

    /**
     * @throws Exception if failed.
     */
    public void testSetAttribute() throws Exception {
        for (int i = 0; i < EXEC_COUNT; i++)
            checkTask(i);
    }

    /**
     * @throws Exception if failed.
     */
    public void testMultiThreaded() throws Exception {
        final GridThreadSerialNumber sNum = new GridThreadSerialNumber();

        final AtomicBoolean failed = new AtomicBoolean(false);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int num = sNum.get();

                try {
                    checkTask(num);
                }
                catch (Throwable e) {
                    error("Failed to execute task.", e);

                    failed.set(true);
                }
            }
        }, EXEC_COUNT, "grid-session-test");

        if (failed.get())
            fail();
    }

    /**
     * @param num Number.
     * @throws GridException if failed.
     */
    private void checkTask(int num) throws GridException {
        Ignite ignite = G.grid(getTestGridName());

        GridComputeTaskFuture<?> fut = executeAsync(ignite.compute(), GridTaskSessionTestTask.class, num);

        Object res = fut.get();

        assert (Integer)res == SPLIT_COUNT - 1 : "Invalid result [num=" + num + ", fut=" + fut + ']';
    }

    /**
     *
     */
    @GridComputeTaskSessionFullSupport
    private static class GridTaskSessionTestTask extends GridComputeTaskSplitAdapter<Serializable, Integer> {
        /** */
        @GridLoggerResource
        private GridLogger log;

        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Serializable arg) throws GridException {
            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            Collection<ComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 1; i <= SPLIT_COUNT; i++) {
                jobs.add(new ComputeJobAdapter(i) {
                    @Override public Serializable execute() throws GridException {
                        assert taskSes != null;

                        int arg = this.<Integer>argument(0);

                        if (log.isInfoEnabled())
                            log.info("Computing job [job=" + this + ", arg=" + arg + ']');

                        if (arg == 1) {
                            if (log.isInfoEnabled())
                                log.info("Set attribute 'testName'.");

                            taskSes.setAttribute("testName", "testVal");
                        }
                        else {
                            try {
                                String val = (String)taskSes.waitForAttribute("testName", 100000);

                                if (log.isInfoEnabled())
                                    log.info("Received attribute 'testName': " + val);

                                if ("testVal".equals(val))
                                    return 1;
                            }
                            catch (InterruptedException e) {
                                throw new GridException("Failed to get attribute due to interruption.", e);
                            }
                        }

                        return 0;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult result, List<GridComputeJobResult> received)
            throws GridException {
            if (result.getException() != null)
                throw result.getException();

            return received.size() == SPLIT_COUNT ? GridComputeJobResultPolicy.REDUCE : GridComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
            if (log.isInfoEnabled())
                log.info("Reducing job [job=" + this + ", results=" + results + ']');

            if (results.size() < SPLIT_COUNT)
                fail();

            int sum = 0;

            for (GridComputeJobResult result : results) {
                if (result.getData() != null)
                    sum += (Integer)result.getData();
            }

            return sum;
        }
    }
}
