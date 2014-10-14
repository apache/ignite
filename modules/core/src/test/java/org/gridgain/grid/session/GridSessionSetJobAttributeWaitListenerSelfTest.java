/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.session;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
@GridCommonTest(group = "Task Session")
public class GridSessionSetJobAttributeWaitListenerSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int SPLIT_COUNT = 5;

    /** */
    private static final long WAIT_TIME = 20000;

    /** */
    private static volatile CountDownLatch startSignal;

    /** */
    public GridSessionSetJobAttributeWaitListenerSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        c.setExecutorService(
            new ThreadPoolExecutor(
                SPLIT_COUNT * 2,
                SPLIT_COUNT * 2,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));

        c.setExecutorServiceShutdown(true);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSetAttribute() throws Exception {
        Grid grid = G.grid(getTestGridName());

        grid.compute().localDeployTask(GridTaskSessionTestTask.class, GridTaskSessionTestTask.class.getClassLoader());

        for (int i = 0; i < 5; i++) {
            refreshInitialData();

            GridComputeTaskFuture<?> fut = grid.compute().execute(GridTaskSessionTestTask.class.getName(), null);

            assert fut != null;

            try {
                // Wait until jobs begin execution.
                boolean await = startSignal.await(WAIT_TIME, TimeUnit.MILLISECONDS);

                assert await == true : "Jobs did not start.";

                Object res = fut.get();

                assert (Integer)res == SPLIT_COUNT : "Invalid result [i=" + i + ", fut=" + fut + ']';
            }
            finally {
                // We must wait for the jobs to be sure that they have completed
                // their execution since they use static variable (shared for the tests).
                fut.get();
            }
        }
    }

    /** */
    private void refreshInitialData() {
        startSignal = new CountDownLatch(SPLIT_COUNT);
    }

    /**
     *
     */
    @GridComputeTaskSessionFullSupport
    public static class GridTaskSessionTestTask extends GridComputeTaskSplitAdapter<Serializable, Integer> {
        /** */
        @GridLoggerResource private GridLogger log;

        /** */
        @GridTaskSessionResource private GridComputeTaskSession taskSes;

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Serializable arg) throws GridException {
            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            Collection<GridComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 1; i <= SPLIT_COUNT; i++) {
                jobs.add(new GridComputeJobAdapter(i) {
                    @SuppressWarnings({"UnconditionalWait"})
                    public Serializable execute() throws GridException {
                        assert taskSes != null;

                        if (log.isInfoEnabled())
                            log.info("Computing job [job=" + this + ", arg=" + argument(0) + ']');

                        startSignal.countDown();

                        try {
                            if (startSignal.await(WAIT_TIME, TimeUnit.MILLISECONDS) == false)
                                fail();

                            GridTaskSessionAttributeTestListener lsnr =
                                new GridTaskSessionAttributeTestListener(log);

                            taskSes.addAttributeListener(lsnr, false);

                            if (log.isInfoEnabled())
                                log.info("Set attribute 'testName'.");

                            taskSes.setAttribute("testName", "testVal");

                            synchronized (lsnr) {
                                lsnr.wait(WAIT_TIME);
                            }

                            return lsnr.getAttributes().size() == 0 ? 0 : 1;
                        }
                        catch (InterruptedException e) {
                            throw new GridException("Failed to wait for listener due to interruption.", e);
                        }
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult result, List<GridComputeJobResult> received) throws GridException {
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

    /**
     *
     */
    private static class GridTaskSessionAttributeTestListener implements GridComputeTaskSessionAttributeListener {
        /** */
        private Map<Object, Object> attrs = new HashMap<>();

        /** */
        private GridLogger log;

        /**
         * @param log Grid logger.
         */
        GridTaskSessionAttributeTestListener(GridLogger log) {
            assert log != null;

            this.log = log;
        }

        /** {@inheritDoc} */
        @Override public synchronized void onAttributeSet(Object key, Object val) {
            assert key != null;

            if (log.isInfoEnabled())
                log.info("Received attribute [name=" + key + ", val=" + val + ']');

            attrs.put(key, val);

            notifyAll();
        }

        /**
         * Getter for property 'attrs'.
         *
         * @return Attributes map.
         */
        public synchronized Map<Object, Object> getAttributes() {
            return attrs;
        }
    }
}
