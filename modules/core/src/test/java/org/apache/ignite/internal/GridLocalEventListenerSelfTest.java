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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test ensuring that event listeners are picked by started node.
 */
public class GridLocalEventListenerSelfTest extends GridCommonAbstractTest {
    /** Whether event fired. */
    private final CountDownLatch fired = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        int idx = getTestGridIndex(gridName);

        if (idx == 0) {
            Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

            lsnrs.put(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    fired.countDown();

                    return true;
                }
            }, new int[] { EventType.EVT_NODE_JOINED } );

            cfg.setLocalEventListeners(lsnrs);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * Test listeners notification.
     *
     * @throws Exception If failed.
     */
    public void testListener() throws Exception {
        startGrids(2);

        assert fired.await(5000, TimeUnit.MILLISECONDS);
    }
}