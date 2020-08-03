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

package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test to reproduce IGNITE-4060.
 */
public class IgniteRoundRobinErrorAfterClientReconnectTest extends GridCommonAbstractTest {
    /** Server index. */
    private static final int SRV_IDX = 0;

    /** Client index. */
    private static final int CLI_IDX = 1;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid(SRV_IDX);
        startClientGrid(CLI_IDX);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60000;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnect() throws Exception {
        final Ignite cli = grid(CLI_IDX);

        final GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

        cli.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                try {
                    cli.compute().apply(new IgniteClosure<String, Void>() {
                        @Override public Void apply(String arg) {
                            return null;
                        }
                    }, "Hello!");

                    fut.onDone(true);

                    return true;
                }
                catch (Exception e) {
                    fut.onDone(e);

                    return false;
                }
            }
        }, EventType.EVT_CLIENT_NODE_RECONNECTED);

        stopGrid(SRV_IDX);
        startGrid(SRV_IDX);

        assert fut.get();
    }
}
