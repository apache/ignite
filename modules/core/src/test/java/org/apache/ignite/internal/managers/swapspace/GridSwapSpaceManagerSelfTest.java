/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.swapspace;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.SwapSpaceEvent;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.swapspace.SwapKey;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVTS_SWAPSPACE;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_CLEARED;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_READ;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_REMOVED;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_STORED;

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

        c.setSwapSpaceSpi(new FileSwapSpaceSpi());

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

        return ((IgniteKernal) ignite).context().swap();
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

        ignite.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt instanceof SwapSpaceEvent;

                info("Received event: " + evt);

                SwapSpaceEvent e = (SwapSpaceEvent) evt;

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

        SwapKey key = new SwapKey("key1");

        String val = "value";

        mgr.write(spaceName, key, val.getBytes(), null);

        mgr.write(spaceName, new SwapKey("key2"), val.getBytes(), null);

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