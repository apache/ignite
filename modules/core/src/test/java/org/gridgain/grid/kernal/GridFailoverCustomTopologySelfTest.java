/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.apache.ignite.spi.failover.*;
import org.apache.ignite.spi.failover.always.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Test failover and custom topology. Topology returns local node if remote node fails.
 */
@GridCommonTest(group = "Kernal Self")
public class GridFailoverCustomTopologySelfTest extends GridCommonAbstractTest {
    /** */
    private final AtomicInteger failCnt = new AtomicInteger(0);

    /** */
    private static final Object mux = new Object();

    /** */
    public GridFailoverCustomTopologySelfTest() {
        super(/*start Grid*/false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setNodeId(null);

        cfg.setFailoverSpi(new AlwaysFailoverSpi() {
            /** {@inheritDoc} */
            @Override public ClusterNode failover(FailoverContext ctx, List<ClusterNode> top) {
                failCnt.incrementAndGet();

                return super.failover(ctx, top);
            }
        });

        return cfg;
    }
    /**
     * Tests that failover don't pick local node if it has been excluded from topology.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"WaitNotInLoop", "UnconditionalWait", "unchecked"})
    public void testFailoverTopology() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            assert ignite1 != null;
            assert ignite2 != null;

            ignite1.compute().localDeployTask(JobTask.class, JobTask.class.getClassLoader());

            try {
                ComputeTaskFuture<String> fut;

                synchronized(mux){
                    IgniteCompute comp = ignite1.compute().enableAsync();

                    comp.execute(JobTask.class, null);

                    fut = comp.future();

                    mux.wait();
                }

                stopAndCancelGrid(2);

                String res = fut.get();

                info("Task result: " + res);
            }
            catch (GridException e) {
                info("Got unexpected grid exception: " + e);
            }

            info("Failed over: " + failCnt.get());

            assert failCnt.get() == 1 : "Invalid fail over counter [expected=1, actual=" + failCnt.get() + ']';
        }
        finally {
            stopGrid(1);

            // Stopping stopped instance just in case.
            stopGrid(2);
        }
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class JobTask extends ComputeTaskAdapter<String, String> {
        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

         /** */
        @IgniteLocalNodeIdResource
        private UUID locNodeId;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) throws GridException {
            assert locNodeId != null;

            if (log.isInfoEnabled())
                log.info("Mapping jobs [subgrid=" + subgrid + ", arg=" + arg + ']');

            ClusterNode remoteNode = null;

            for (ClusterNode node : subgrid) {
                if (!node.id().equals(locNodeId))
                    remoteNode = node;
            }

            return Collections.singletonMap(new ComputeJobAdapter(locNodeId) {
                /** */
               @IgniteLocalNodeIdResource
               private UUID nodeId;

                /** {@inheritDoc} */
                @SuppressWarnings("NakedNotify")
                @Override public Serializable execute() throws GridException {
                    assert nodeId != null;

                    if (!nodeId.equals(argument(0))) {
                        try {
                            synchronized(mux) {
                                mux.notifyAll();
                            }

                            Thread.sleep(Integer.MAX_VALUE);
                        }
                        catch (InterruptedException e) {
                            throw new ComputeExecutionRejectedException("Expected interruption during execution.", e);
                        }
                    }
                    else
                        return "success";

                    throw new ComputeExecutionRejectedException("Expected exception during execution.");
                }
            }, remoteNode);
        }

        /** {@inheritDoc} */
        @Override public String reduce(List<ComputeJobResult> results) throws GridException {
            assert results.size() == 1;

            return results.get(0).getData();
        }
    }
}
