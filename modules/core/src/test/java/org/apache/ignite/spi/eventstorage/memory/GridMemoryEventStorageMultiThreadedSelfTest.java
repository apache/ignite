/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.eventstorage.memory;

import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

/**
 * Memory event storage load test.
 */
@GridSpiTest(spi = MemoryEventStorageSpi.class, group = "EventStorage SPI")
public class GridMemoryEventStorageMultiThreadedSelfTest extends GridSpiAbstractTest<MemoryEventStorageSpi> {
    /**
     * @throws Exception If test failed
     */
    @Test
    public void testMultiThreaded() throws Exception {
        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < 100000; i++)
                    getSpi().record(new DiscoveryEvent(null, "Test event", 1, null));

                return null;
            }
        }, 10, "event-thread");

        Collection<Event> evts = getSpi().localEvents(F.<Event>alwaysTrue());

        info("Events count in memory: " + evts.size());

        assert evts.size() <= 10000 : "Incorrect number of events: " + evts.size();
    }
}
