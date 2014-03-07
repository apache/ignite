/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import com.sun.org.apache.commons.collections.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Test for {@link GridNodeShadow}.
 */
@GridCommonTest(group = "Kernal Self")
public class GridNodeShadowSelfTest extends GridCommonAbstractTest {
    /** Exchanger between junit and event listener threads. */
    private Exchanger<Map<Integer, GridNodeShadow>> ex = new Exchanger<>();

    /** Special message for {@code TestEventListener} to signal that something went wrong. */
    private static final int DIRTY_MSG_KEY = -1;

    /**
     * @throws Exception If test failed.
     */
    public void testGridNodeShadow() throws Exception {
        try {
            Grid g1 = startGrid(1);

            g1.events().localListen(new TestEventListener(), EVTS_DISCOVERY);

            Grid g2 = startGrid(2);

            GridNode node = g2.localNode();

            stopGrid(2);

            Map<Integer, GridNodeShadow> evtsToShadows = ex.exchange(null, getTestTimeout(), SECONDS);

            assertFalse(evtsToShadows.containsKey(DIRTY_MSG_KEY));
            assertTrue(evtsToShadows.containsKey(EVT_NODE_JOINED));
            assertTrue(evtsToShadows.containsKey(EVT_NODE_LEFT));

            for (Map.Entry<Integer, GridNodeShadow> evtToShadow : evtsToShadows.entrySet()) {
                GridNodeShadow sh = evtToShadow.getValue();

                assertEquals(node.id(), sh.id());
                assertTrue(CollectionUtils.isEqualCollection(node.addresses(), sh.addresses()));
                assertTrue(CollectionUtils.isEqualCollection(node.hostNames(), sh.hostNames()));

                Map<String, Object> shAttrs = sh.attributes();

                for (Map.Entry<String, Object> e : node.attributes().entrySet()) {
                    Object value = e.getValue();

                    if (value != null) {
                        if (value.getClass().isArray())
                            assertTrue(Arrays.equals((Object[])value, (Object[])shAttrs.get(e.getKey())));
                        else
                            assertEquals(value, shAttrs.get(e.getKey()));
                    }
                    else
                        assertNull(shAttrs.get(e.getKey()));
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Event listener to track grid local events.
     */
    private class TestEventListener implements GridPredicate<GridEvent> {
        /** Map storing all collected events (ids) and theirs shadows. */
        private final Map<Integer, GridNodeShadow> evtsToShadows = new HashMap<>();

        /** {@inheritDoc} */
        @Override public boolean apply(GridEvent evt) {
            if (!(evt instanceof GridDiscoveryEvent)) {
                error("Received unexpected event in TestEventListener: " + evt);

                evtsToShadows.put(DIRTY_MSG_KEY, null);

                return true;
            }

            GridDiscoveryEvent evt0 = (GridDiscoveryEvent)evt;

            GridNodeShadow sh = evt0.shadow();

            evtsToShadows.put(evt0.type(), sh);

            if (evt0.type() == EVT_NODE_LEFT) {
                try {
                    ex.exchange(evtsToShadows, getTestTimeout(), SECONDS);
                }
                catch (Exception e) {
                    error(e.getMessage());

                    evtsToShadows.put(DIRTY_MSG_KEY, null);
                }
            }

            return true;
        }
    }
}
