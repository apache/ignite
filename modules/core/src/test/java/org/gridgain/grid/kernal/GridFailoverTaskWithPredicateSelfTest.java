package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.grid.spi.failover.always.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Test failover of a task with Node filter predicate.
 */
@GridCommonTest(group = "Kernal Self")
public class GridFailoverTaskWithPredicateSelfTest extends GridCommonAbstractTest {
    /** First node's name. */
    private static final String NODE1 = "NODE1";

    /** Second node's name. */
    private static final String NODE2 = "NODE2";

    /** Third node's name. */
    private static final String NODE3 = "NODE3";

    /** Predicate to exclude the second node from topology */
    private final GridPredicate<ClusterNode> p = new GridPredicate<ClusterNode>() {
        @Override
        public boolean apply(ClusterNode e) {
            return !NODE2.equals(e.attribute(GridNodeAttributes.ATTR_GRID_NAME));
        }
    };

    /** Whether delegating fail over node was found or not. */
    private final AtomicBoolean routed = new AtomicBoolean();

    /** Whether job execution failed with exception. */
    private final AtomicBoolean failed = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailoverSpi(new GridAlwaysFailoverSpi() {
            /** {@inheritDoc} */
            @Override public ClusterNode failover(GridFailoverContext ctx, List<ClusterNode> grid) {
                ClusterNode failoverNode = super.failover(ctx, grid);

                if (failoverNode != null)
                    routed.set(true);
                else
                    routed.set(false);

                return failoverNode;
            }
        });

        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        return cfg;
    }

    /**
     * Tests that failover doesn't happen on two-node grid when the Task is applicable only for the first node
     * and fails on it.
     *
     * @throws Exception If failed.
     */
    public void testJobNotFailedOver() throws Exception {
        failed.set(false);
        routed.set(false);

        try {
            Ignite ignite1 = startGrid(NODE1);
            Ignite ignite2 = startGrid(NODE2);

            assert ignite1 != null;
            assert ignite2 != null;

            compute(ignite1.cluster().forPredicate(p)).withTimeout(10000).execute(JobFailTask.class.getName(), "1");
        }
        catch (GridTopologyException ignored) {
            failed.set(true);
        }
        finally {
            assertTrue(failed.get());
            assertFalse(routed.get());

            stopGrid(NODE1);
            stopGrid(NODE2);
        }
    }

    /**
     * Tests that failover happens on three-node grid when the Task is applicable for the first node
     * and fails on it, but is also applicable on another node.
     *
     * @throws Exception If failed.
     */
    public void testJobFailedOver() throws Exception {
        failed.set(false);
        routed.set(false);

        try {
            Ignite ignite1 = startGrid(NODE1);
            Ignite ignite2 = startGrid(NODE2);
            Ignite ignite3 = startGrid(NODE3);

            assert ignite1 != null;
            assert ignite2 != null;
            assert ignite3 != null;

            Integer res = (Integer)compute(ignite1.cluster().forPredicate(p)).withTimeout(10000).
                execute(JobFailTask.class.getName(), "1");

            assert res == 1;
        }
        catch (GridTopologyException ignored) {
            failed.set(true);
        }
        finally {
            assertFalse(failed.get());
            assertTrue(routed.get());

            stopGrid(NODE1);
            stopGrid(NODE2);
            stopGrid(NODE3);
        }
    }

    /**
     * Tests that in case of failover our predicate is intersected with projection
     * (logical AND is performed).
     *
     * @throws Exception If error happens.
     */
    public void testJobNotFailedOverWithStaticProjection() throws Exception {
        failed.set(false);
        routed.set(false);

        try {
            Ignite ignite1 = startGrid(NODE1);
            Ignite ignite2 = startGrid(NODE2);
            Ignite ignite3 = startGrid(NODE3);

            assert ignite1 != null;
            assert ignite2 != null;
            assert ignite3 != null;

            // Get projection only for first 2 nodes.
            GridProjection nodes = ignite1.cluster().forNodeIds(Arrays.asList(
                ignite1.cluster().localNode().id(),
                ignite2.cluster().localNode().id()));

            // On failover NODE3 shouldn't be taken into account.
            Integer res = (Integer)compute(nodes.forPredicate(p)).withTimeout(10000).
                execute(JobFailTask.class.getName(), "1");

            assert res == 1;
        }
        catch (GridTopologyException ignored) {
            failed.set(true);
        }
        finally {
            assertTrue(failed.get());
            assertFalse(routed.get());

            stopGrid(NODE1);
            stopGrid(NODE2);
            stopGrid(NODE3);
        }
    }

    /** */
    @GridComputeTaskSessionFullSupport
    private static class JobFailTask implements GridComputeTask<String, Object> {
        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession ses;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) throws GridException {
            ses.setAttribute("fail", true);

            return Collections.singletonMap(new GridComputeJobAdapter(arg) {
                /** Local node ID. */
                @GridLocalNodeIdResource
                private UUID locId;

                /** {@inheritDoc} */
                @SuppressWarnings({"RedundantTypeArguments"})
                @Override
                public Serializable execute() throws GridException {
                    boolean fail;

                    try {
                        fail = ses.<String, Boolean>waitForAttribute("fail", 0);
                    }
                    catch (InterruptedException e) {
                        throw new GridException("Got interrupted while waiting for attribute to be set.", e);
                    }

                    if (fail) {
                        ses.setAttribute("fail", false);

                        throw new GridException("Job exception.");
                    }

                    // This job does not return any result.
                    return Integer.parseInt(this.<String>argument(0));
                }
            }, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> received)
                throws GridException {
            if (res.getException() != null && !(res.getException() instanceof GridComputeUserUndeclaredException))
                return GridComputeJobResultPolicy.FAILOVER;

            return GridComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            assert results.size() == 1;

            return results.get(0).getData();
        }
    }

}
