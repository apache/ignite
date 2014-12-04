/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.swapspace;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.swapspace.*;
import org.gridgain.grid.spi.swapspace.file.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.GridEventType.*;

/**
 * Tests for {@link GridSwapSpaceManager}.
 */
@SuppressWarnings({"ProhibitedExceptionThrown"})
@GridCommonTest(group = "Kernal Self")
public class GridSwapSpaceManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String spaceName = "swapspace_mgr";

    /**
     *
     */
    public GridSwapSpaceManagerSelfTest() {
        super(/*start grid*/true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setSwapSpaceSpi(new GridFileSwapSpaceSpi());

        return c;
    }

    /**
     * Returns swap space manager instance for given Grid.
     *
     * @param ignite Grid instance.
     * @return Swap space manager.
     */
    private GridSwapSpaceManager getSwapSpaceManager(Ignite ignite) {
        assert ignite != null;

        return ((GridKernal) ignite).context().swap();
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSize() throws Exception {
        final Ignite ignite = grid();

        final CountDownLatch clearCnt = new CountDownLatch(1);
        final CountDownLatch readCnt = new CountDownLatch(1);
        final CountDownLatch storeCnt = new CountDownLatch(2);
        final CountDownLatch rmvCnt = new CountDownLatch(1);

        ignite.events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                assert evt instanceof GridSwapSpaceEvent;

                info("Received event: " + evt);

                GridSwapSpaceEvent e = (GridSwapSpaceEvent) evt;

                assert spaceName.equals(e.space());
                assert ignite.cluster().localNode().id().equals(e.node().id());

                switch (evt.type()) {
                    case EVT_SWAP_SPACE_CLEARED:
                        clearCnt.countDown();

                        break;

                    case EVT_SWAP_SPACE_DATA_READ:
                        readCnt.countDown();

                        break;

                    case EVT_SWAP_SPACE_DATA_STORED:
                        storeCnt.countDown();

                        break;

                    case EVT_SWAP_SPACE_DATA_REMOVED:
                        rmvCnt.countDown();

                        break;

                    default:
                        assert false : "Unexpected event: " + evt;
                }

                return true;
            }
        }, EVTS_SWAPSPACE);

        GridSwapSpaceManager mgr = getSwapSpaceManager(ignite);

        assert mgr != null;

        // Empty data space.
        assertEquals(0, mgr.swapSize(spaceName));

        GridSwapKey key = new GridSwapKey("key1");

        String val = "value";

        mgr.write(spaceName, key, val.getBytes(), null);

        mgr.write(spaceName, new GridSwapKey("key2"), val.getBytes(), null);

        assert storeCnt.await(10, SECONDS);

        byte[] arr = mgr.read(spaceName, key, null);

        assert arr != null;

        assert val.equals(new String(arr));

        final GridTuple<Boolean> b = F.t(false);

        mgr.remove(spaceName, key, new CI1<byte[]>() {
            @Override public void apply(byte[] rmv) {
                b.set(rmv != null);
            }
        }, null);

        assert b.get();

        assert rmvCnt.await(10, SECONDS);
        assert readCnt.await(10, SECONDS);

        mgr.clear(spaceName);

        assert clearCnt.await(10, SECONDS) : "Count: " + clearCnt.getCount();
    }
}
