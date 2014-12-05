package org.gridgain.grid.kernal;

import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test whether internal and visor tasks are routed to management pool.
 */
@GridCommonTest(group = "Kernal Self")
public class GridManagementJobSelfTest extends GridCommonAbstractTest {
    /** Amount of nodes in the grid. */
    private static final int GRID_CNT = 3;

    /** Management pool threads name prefix. */
    private static final String MGMT_THREAD_PREFIX = "mgmt_thread_";

    /** Name of a regular task. */
    private static final String TASK_NAME = "task";

    /** IP finder. */
    private final TcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /**
     * Do not start grid initially.
     */
    public GridManagementJobSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        ExecutorService mgmtExecutor = Executors.newFixedThreadPool(10, new ThreadFactory() {
            /** Counter for unique thread names. */
            private AtomicLong ctr = new AtomicLong();

            /** {@inheritDoc} */
            @SuppressWarnings("NullableProblems")
            @Override public Thread newThread(Runnable r) {
                Thread t = new Thread(r);

                t.setName(MGMT_THREAD_PREFIX + ctr.getAndIncrement());

                return t;
            }
        });

        cfg.setManagementExecutorService(mgmtExecutor);

        cfg.setManagementExecutorServiceShutdown(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Ensure that regular tasks are executed within public pool while Visor and internal
     * taskss are executed in management pool on remote nodes.
     *
     * @throws Exception If failed.
     */
    public void testNamedTasks() throws Exception {
        runJob(TASK_NAME, new TestJob());
    }

    /**
     * Ensure that jobs annotated with {@link GridInternal} are always executed in
     * management pool irrespective of task name.
     *
     * @throws Exception If failed.
     */
    public void testAnnotatedTasks() throws Exception {
        runJob(TASK_NAME, new TestJobInternal());
    }

    /**
     * Execute the TestJob on remote nodes.
     *
     * @param taskName Name of the task in which context this job will be executed.
     * @param job Job.
     * @throws Exception If failed.
     */
    private void runJob(String taskName, Callable<Object> job) throws Exception {
        // We run a task on remote nodes because on local node jobs will be executed in system pool anyway.
        compute(grid(0).forRemotes()).withName(taskName).call(job);
    }

    /**
     *  Test job which ensures that its executor thread is from management pool in case
     *  task name corresponds to either internal or Visor task.
     */
    private static class TestJob implements Callable<Object>, Serializable {
        /** Task session. */
        @IgniteTaskSessionResource
        protected ComputeTaskSession ses;

        /** {@inheritDoc} */
        @Nullable @Override public Object call() throws GridException {
            String threadName = Thread.currentThread().getName();

            assertFalse(threadName.startsWith(MGMT_THREAD_PREFIX));

            return null;
        }
    }

    /**
     * Test job which ensures that it is always executed in management pool irrespectively
     * of task name due to presence of {@link GridInternal} annotation.
     */
    @GridInternal
    private static class TestJobInternal implements Callable<Object>, Serializable {
        /** {@inheritDoc} */
        @Nullable @Override public Object call() throws GridException {
            String threadName = Thread.currentThread().getName();

            assertTrue(threadName.startsWith(MGMT_THREAD_PREFIX));

            return null;
        }
    }
}
