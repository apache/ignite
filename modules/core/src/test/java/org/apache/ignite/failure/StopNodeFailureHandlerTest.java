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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * {@link StopNodeFailureHandler} tests.
 */
public class StopNodeFailureHandlerTest extends GridCommonAbstractTest {
    /**
     * Tests node is stopped after triggering StopNodeFailureHandler.
     */
    public void testNodeStopped() throws Exception {
        try {
            IgniteEx ignite = startGrid(0);

            final CountDownLatch latch = new CountDownLatch(1);

            ignite.context().failure().process(
                new FailureContext(FailureType.CRITICAL_ERROR, null),
                new StopNodeFailureHandler() {
                    @Override void onDone() {
                        latch.countDown();
                    }
                });

            assert latch.await(10000, TimeUnit.MILLISECONDS) : "FailureHandler seems not triggered";

            assert IgnitionEx.state(ignite.name()) == IgniteState.STOPPED_ON_FAILURE;
        }
        finally {
            stopAllGrids();
        }
    }
}
