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

package org.apache.ignite.internal.managers.events;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.events.IgniteEventType.*;

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        int[] evts = new int[EVTS_ALL.length + 1];

        evts[0] = Integer.MAX_VALUE - 1;

        System.arraycopy(EVTS_ALL, 0, evts, 1, EVTS_ALL.length);

        cfg.setIncludeEventTypes(evts);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitForEvent() throws Exception {
        Ignite ignite = grid();

        final int usrType = Integer.MAX_VALUE - 1;

        IgniteInternalFuture<IgniteEvent> fut = waitForLocalEvent(ignite.events(), new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent e) {
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

        ignite.events().recordLocal(new IgniteEventAdapter(null, "Test message.", usrType) {
            // No-op.
        });

        assert fut.get() != null;

        assertEquals(usrType, fut.get().type());
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitForEventContinuationTimeout() throws Exception {
        Ignite ignite = grid();

        try {
            // We'll never wait for nonexistent type of event.
            int usrType = Integer.MAX_VALUE - 1;

            waitForLocalEvent(ignite.events(), F.<IgniteEvent>alwaysTrue(), usrType).get(1000);

            fail("GridFutureTimeoutException must have been thrown.");
        }
        catch (IgniteFutureTimeoutException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUserEvent() throws Exception {
        Ignite ignite = grid();

        try {
            ignite.events().recordLocal(new IgniteEventAdapter(null, "Test message.", IgniteEventType.EVT_NODE_FAILED) {
                // No-op.
            });

            assert false : "Exception should have been thrown.";

        }
        catch (IllegalArgumentException e) {
            info("Caught expected exception: " + e);
        }
    }
}
