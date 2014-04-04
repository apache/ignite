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

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        assert NODES_CNT > 2;

        ids = new LinkedList<>();

        for (int i = 0; i < NODES_CNT; i++) {
            Grid g = startGrid(i);

            ids.add(g.localNode().id());
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
}
