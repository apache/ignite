/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Node filter test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridNodeFilterSelfTest extends GridCommonAbstractTest {
    /** Grid instance. */
    private Grid grid;

    /** Remote instance. */
    private Grid rmtGrid;

    /** */
    public GridNodeFilterSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        grid = startGrid(1);

        rmtGrid = startGrid(2);
        startGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid(1);
        stopGrid(2);
        stopGrid(3);

        grid = null;
        rmtGrid = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousExecute() throws Exception {
        UUID nodeId = grid.cluster().localNode().id();

        UUID rmtNodeId = rmtGrid.cluster().localNode().id();

        Collection<GridNode> locNodes = grid.cluster().forNodeId(nodeId).nodes();

        assert locNodes.size() == 1;
        assert locNodes.iterator().next().id().equals(nodeId);

        Collection<GridNode> rmtNodes = grid.cluster().forNodeId(rmtNodeId).nodes();

        assert rmtNodes.size() == 1;
        assert rmtNodes.iterator().next().id().equals(rmtNodeId);
    }
}
