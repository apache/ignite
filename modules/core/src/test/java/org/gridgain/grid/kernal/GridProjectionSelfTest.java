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
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Test for {@link org.apache.ignite.cluster.ClusterGroup}.
 */
@GridCommonTest(group = "Kernal Self")
public class GridProjectionSelfTest extends GridProjectionAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 4;

    /** Projection node IDs. */
    private static Collection<UUID> ids;

    /** */
    private static Ignite ignite;

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        assert NODES_CNT > 2;

        ids = new LinkedList<>();

        for (int i = 0; i < NODES_CNT; i++) {
            Ignite g = startGrid(i);

            ids.add(g.cluster().localNode().id());

            if (i == 0)
                ignite = g;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        for (int i = 0; i < NODES_CNT; i++)
            stopGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected ClusterGroup projection() {
        return grid(0).forPredicate(F.<ClusterNode>nodeForNodeIds(ids));
    }

    /** {@inheritDoc} */
    @Override protected UUID localNodeId() {
        return grid(0).localNode().id();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandom() throws Exception {
        assertTrue(ignite.cluster().nodes().contains(ignite.cluster().forRandom().node()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOldest() throws Exception {
        ClusterGroup oldest = ignite.cluster().forOldest();

        ClusterNode node = null;

        long minOrder = Long.MAX_VALUE;

        for (ClusterNode n : ignite.cluster().nodes()) {
            if (n.order() < minOrder) {
                node = n;

                minOrder = n.order();
            }
        }

        assertEquals(oldest.node(), ignite.cluster().forNode(node).node());
    }

    /**
     * @throws Exception If failed.
     */
    public void testYoungest() throws Exception {
        ClusterGroup youngest = ignite.cluster().forYoungest();

        ClusterNode node = null;

        long maxOrder = Long.MIN_VALUE;

        for (ClusterNode n : ignite.cluster().nodes()) {
            if (n.order() > maxOrder) {
                node = n;

                maxOrder = n.order();
            }
        }

        assertEquals(youngest.node(), ignite.cluster().forNode(node).node());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNewNodes() throws Exception {
        ClusterGroup youngest = ignite.cluster().forYoungest();
        ClusterGroup oldest = ignite.cluster().forOldest();

        ClusterNode old = oldest.node();
        ClusterNode last = youngest.node();

        assertNotNull(last);

        try (Ignite g = startGrid(NODES_CNT)) {
            ClusterNode n = g.cluster().localNode();

            ClusterNode latest = youngest.node();

            assertNotNull(latest);
            assertEquals(latest.id(), n.id());
            assertEquals(oldest.node(), old);
        }
    }
}
