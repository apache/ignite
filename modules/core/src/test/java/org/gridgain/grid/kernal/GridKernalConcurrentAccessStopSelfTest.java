/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.testframework.junits.common.*;

import static org.apache.ignite.events.GridEventType.*;

/**
 * Tests kernal stop while it is being accessed from asynchronous even listener.
 */
public class GridKernalConcurrentAccessStopSelfTest  extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRIDS = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRIDS; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        for (int i = GRIDS; i-- >= 0;)
            stopGrid(i);
    }

    /**
     *
     */
    public void testConcurrentAccess() {
        for (int i = 0; i < GRIDS; i++) {
            grid(i).events().localListen(new IgnitePredicate<IgniteEvent>() {
                @Override public boolean apply(IgniteEvent evt) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    return true;
                }
            }, EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED);
        }
    }
}
