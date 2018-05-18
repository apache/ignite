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

import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Tests kernal stop while it is being accessed from asynchronous even listener.
 */
public class GridKernalConcurrentAccessStopSelfTest  extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRIDS = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRIDS; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        for (int i = GRIDS; i-- >= 0;)
            stopGrid(i);
    }

    /**
     *
     */
    public void testConcurrentAccess() {
        for (int i = 0; i < GRIDS; i++) {
            grid(i).events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    return true;
                }
            }, EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED);
        }
    }
}