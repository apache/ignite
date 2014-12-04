/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Test ensuring that event listeners are picked by started node.
 */
public class GridLocalEventListenerSelfTest extends GridCommonAbstractTest {
    /** Whether event fired. */
    private final CountDownLatch fired = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        int idx = getTestGridIndex(gridName);

        if (idx == 0) {
            Map<IgnitePredicate<? extends IgniteEvent>, int[]> lsnrs = new HashMap<>();

            lsnrs.put(new IgnitePredicate<IgniteEvent>() {
                @Override public boolean apply(IgniteEvent evt) {
                    fired.countDown();

                    return true;
                }
            }, new int[] { IgniteEventType.EVT_NODE_JOINED } );

            cfg.setLocalEventListeners(lsnrs);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * Test listeners notification.
     *
     * @throws Exception If failed.
     */
    public void testListener() throws Exception {
        startGrids(2);

        assert fired.await(5000, TimeUnit.MILLISECONDS);
    }
}
