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

package org.apache.ignite.internal.managers.events;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventAdapter;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVTS_ALL;

/**
 * Tests for {@link GridEventStorageManager}.
 */
public class GridEventStorageManagerSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridEventStorageManagerSelfTest() {
        super(/* start grid */true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int[] evts = new int[EVTS_ALL.length + 1];

        evts[0] = Integer.MAX_VALUE - 1;

        System.arraycopy(EVTS_ALL, 0, evts, 1, EVTS_ALL.length);

        cfg.setIncludeEventTypes(evts);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWaitForEvent() throws Exception {
        Ignite ignite = grid();

        final int usrType = Integer.MAX_VALUE - 1;

        IgniteFuture<Event> fut = waitForLocalEvent(ignite.events(), new IgnitePredicate<Event>() {
            @Override public boolean apply(Event e) {
                return e.type() == usrType;
            }
        }, usrType);

        try {
            fut.get(500);

            fail("GridFutureTimeoutException must have been thrown.");
        }
        catch (IgniteFutureTimeoutException e) {
            info("Caught expected exception: " + e);
        }

        ignite.events().recordLocal(new EventAdapter(null, "Test message.", usrType) {
            // No-op.
        });

        assert fut.get() != null;

        assertEquals(usrType, fut.get().type());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testWaitForEventContinuationTimeout() throws Exception {
        Ignite ignite = grid();

        try {
            // We'll never wait for nonexistent type of event.
            int usrType = Integer.MAX_VALUE - 1;

            waitForLocalEvent(ignite.events(), F.<Event>alwaysTrue(), usrType).get(1000);

            fail("GridFutureTimeoutException must have been thrown.");
        }
        catch (IgniteFutureTimeoutException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUserEvent() throws Exception {
        Ignite ignite = grid();

        try {
            ignite.events().recordLocal(new EventAdapter(null, "Test message.", EventType.EVT_NODE_FAILED) {
                // No-op.
            });

            assert false : "Exception should have been thrown.";

        }
        catch (IllegalArgumentException e) {
            info("Caught expected exception: " + e);
        }
    }
}
