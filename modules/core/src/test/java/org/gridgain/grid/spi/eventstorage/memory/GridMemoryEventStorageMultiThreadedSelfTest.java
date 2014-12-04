/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.eventstorage.memory;

import org.apache.ignite.events.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Memory event storage load test.
 */
@GridSpiTest(spi = GridMemoryEventStorageSpi.class, group = "EventStorage SPI")
public class GridMemoryEventStorageMultiThreadedSelfTest extends GridSpiAbstractTest<GridMemoryEventStorageSpi> {
    /**
     * @throws Exception If test failed
     */
    public void testMultiThreaded() throws Exception {
        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < 100000; i++)
                    getSpi().record(new GridDiscoveryEvent(null, "Test event", 1, null));

                return null;
            }
        }, 10, "event-thread");

        Collection<IgniteEvent> evts = getSpi().localEvents(F.<IgniteEvent>alwaysTrue());

        info("Events count in memory: " + evts.size());

        assert evts.size() <= 10000 : "Incorrect number of events: " + evts.size();
    }
}
