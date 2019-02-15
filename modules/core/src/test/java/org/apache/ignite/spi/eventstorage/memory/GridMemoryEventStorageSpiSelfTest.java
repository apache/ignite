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
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;

/**
 * Tests for {@link MemoryEventStorageSpi}.
 */
@GridSpiTest(spi = MemoryEventStorageSpi.class, group = "Event Storage SPI")
@RunWith(JUnit4.class)
public class GridMemoryEventStorageSpiSelfTest extends GridSpiAbstractTest<MemoryEventStorageSpi> {
    /** */
    private static final int EXPIRE_CNT = 100;

    /**
     * @return Maximum events queue size.
     */
    @GridSpiTestConfig
    public long getExpireCount() {
        return EXPIRE_CNT;
    }

    /**
     * @return Events expiration time.
     */
    @GridSpiTestConfig
    public long getExpireAgeMs() {
        return 1000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMemoryEventStorage() throws Exception {
        MemoryEventStorageSpi spi = getSpi();

        IgnitePredicate<Event> filter = F.alwaysTrue();

        // Get all events.
        Collection<Event> evts = spi.localEvents(filter);

        // Check.
        assert evts != null : "Events can't be null.";
        assert evts.isEmpty() : "Invalid events count.";

        // Store.
        spi.record(createEvent());

        // Get all events.
        evts = spi.localEvents(filter);

        // Check stored events.
        assert evts != null : "Events can't be null.";
        assert evts.size() == 1 : "Invalid events count.";

        // Sleep a bit more than expire age configuration property.
        Thread.sleep(getExpireAgeMs() * 2);

        // Get all events.
        evts = spi.localEvents(filter);

        // Check expired by age.
        assert evts != null : "Events can't be null.";
        assert evts.isEmpty() : "Invalid events count.";

        // Clear.
        spi.clearAll();

        // Get all events.
        evts = spi.localEvents(filter);

        // Check events cleared.
        assert evts != null : "Events can't be null.";
        assert evts.isEmpty() : "Invalid events count.";
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFilter() throws Exception {
        MemoryEventStorageSpi spi = getSpi();

        try {
            spi.clearAll();

            spi.setFilter(F.<Event>alwaysFalse());

            // This event should not record.
            spi.record(createEvent());

            spi.setFilter(null);

            spi.record(createEvent());

            // Get all events.
            Collection<Event> evts = spi.localEvents(F.<Event>alwaysTrue());

            assert evts != null : "Events can't be null.";
            assert evts.size() == 1 : "Invalid events count: " + evts.size();
        }
        finally {
            if (spi != null)
                spi.clearAll();
        }
    }

    /**
     * @return Discovery event.
     * @throws Exception If error occurred.
     */
    private Event createEvent() throws Exception {
        return new DiscoveryEvent(null, "Test Event", EVT_NODE_METRICS_UPDATED, null);
    }
}
