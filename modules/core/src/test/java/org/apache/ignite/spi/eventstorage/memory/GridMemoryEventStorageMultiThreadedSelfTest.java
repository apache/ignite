/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
