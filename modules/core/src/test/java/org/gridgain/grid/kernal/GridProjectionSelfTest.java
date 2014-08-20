/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Test for {@link GridProjection}.
 */
@GridCommonTest(group = "Kernal Self")
public class GridProjectionSelfTest extends GridProjectionAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 4;

    /** Projection node IDs. */
    private static Collection<UUID> ids;

    /** */
    private static Grid grid;

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        assert NODES_CNT > 2;

        ids = new LinkedList<>();

        for (int i = 0; i < NODES_CNT; i++) {
            Grid g = startGrid(i);

            ids.add(g.localNode().id());

            if (i == 0)
                grid = g;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        for (int i = 0; i < NODES_CNT; i++)
            stopGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected GridProjection projection() {
        return grid(0).forPredicate(F.<GridNode>nodeForNodeIds(ids));
    }

    /** {@inheritDoc} */
    @Override protected UUID localNodeId() {
        return grid(0).localNode().id();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandom() throws Exception {
        assertTrue(grid.nodes().contains(grid.forRandom().node()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOldest() throws Exception {
        GridProjection oldest = grid.forOldest();

        GridNode node = null;

        long minOrder = Long.MAX_VALUE;

        for (GridNode n : grid.nodes()) {
            if (n.order() < minOrder) {
                node = n;

                minOrder = n.order();
            }
        }

        assertEquals(oldest.node(), grid.forNode(node).node());
    }

    /**
     * @throws Exception If failed.
     */
    public void testYoungest() throws Exception {
        GridProjection youngest = grid.forYoungest();

        GridNode node = null;

        long maxOrder = Long.MIN_VALUE;

        for (GridNode n : grid.nodes()) {
            if (n.order() > maxOrder) {
                node = n;

                maxOrder = n.order();
            }
        }

        assertEquals(youngest.node(), grid.forNode(node).node());
    }

    /**
     * @throws Exception If failed.
     */
    public void testNewNodes() throws Exception {
        GridProjection youngest = grid.forYoungest();
        GridProjection oldest = grid.forOldest();

        GridNode old = oldest.node();
        GridNode last = youngest.node();

        assertNotNull(last);

        try (Grid g = startGrid(NODES_CNT)) {
            GridNode n = g.localNode();

            GridNode latest = youngest.node();

            assertNotNull(latest);
            assertEquals(latest.id(), n.id());
            assertEquals(oldest.node(), old);
        }
    }
}
