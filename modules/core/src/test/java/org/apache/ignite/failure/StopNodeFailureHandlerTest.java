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

package org.apache.ignite.failure;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteState;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.PE;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * {@link StopNodeFailureHandler} tests.
 */
public class StopNodeFailureHandlerTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return igniteInstanceName.endsWith("1") ?
            new StopNodeFailureHandler() :
            new NoOpFailureHandler();
    }

    /**
     * Tests node is stopped after triggering StopNodeFailureHandler.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeStopped() throws Exception {
        try {
            IgniteEx ignite0 = startGrid(0);
            IgniteEx ignite1 = startGrid(1);

            final CountDownLatch latch = new CountDownLatch(1);

            ignite0.events().localListen(new PE() {
                @Override public boolean apply(Event evt) {
                    latch.countDown();

                    return true;
                }
            }, EventType.EVT_NODE_LEFT);

            ignite1.context().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, null));

            assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));

            Thread.sleep(1000);

            assertEquals(IgnitionEx.state(ignite0.name()), IgniteState.STARTED);
            assertEquals(IgnitionEx.state(ignite1.name()), IgniteState.STOPPED_ON_FAILURE);
        }
        finally {
            stopAllGrids();
        }
    }
}
