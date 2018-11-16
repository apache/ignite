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

package org.apache.ignite.internal.processors.service;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.service.inner.LongInitializedTestService;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests that requests of change service's state won't be missed and will be handled correctly on a coordinator change.
 *
 * It uses {@link LongInitializedTestService} with long running #init method to delay requests processing.
 */
public class ServiceDeploymentProcessingOnCoordinatorChangeTest extends GridCommonAbstractTest {
    /** Timeout to avoid tests hang. */
    private static final long TEST_FUTURE_WAIT_TIMEOUT = 60_000;

    /**
     * @throws Exception In case of an error.
     */
    public void testDeploymentProcessingOnCoordinatorStop() throws Exception {
        try {
            startGrids(4);

            IgniteEx ignite2 = grid(2);

            IgniteFuture fut = ignite2.services().deployNodeSingletonAsync("testService",
                new LongInitializedTestService(5000L));
            IgniteFuture fut2 = ignite2.services().deployNodeSingletonAsync("testService2",
                new LongInitializedTestService(5000L));
            IgniteFuture fut3 = ignite2.services().deployNodeSingletonAsync("testService3",
                new LongInitializedTestService(5000L));

            IgniteEx ignite0 = grid(0);

            assertEquals(ignite0.localNode(), U.oldest(ignite2.cluster().nodes(), null));

            ignite0.close();

            fut.get(TEST_FUTURE_WAIT_TIMEOUT);
            fut2.get(TEST_FUTURE_WAIT_TIMEOUT);
            fut3.get(TEST_FUTURE_WAIT_TIMEOUT);

            IgniteEx ignite3 = grid(3);

            assertNotNull(ignite3.services().service("testService"));
            assertNotNull(ignite3.services().service("testService2"));
            assertNotNull(ignite3.services().service("testService3"));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception In case of an error.
     */
    public void testDeploymentProcessingOnCoordinatorStop2() throws Exception {
        try {
            startGrids(5);

            IgniteEx ignite4 = grid(4);

            IgniteFuture depFut = ignite4.services().deployNodeSingletonAsync("testService",
                new LongInitializedTestService(5000L));
            IgniteFuture depFut2 = ignite4.services().deployNodeSingletonAsync("testService2",
                new LongInitializedTestService(5000L));

            IgniteEx ignite0 = grid(0);

            assertEquals(ignite0.localNode(), U.oldest(ignite4.cluster().nodes(), null));

            ignite0.close();

            depFut.get(getTestTimeout());
            depFut2.get(TEST_FUTURE_WAIT_TIMEOUT);

            Ignite ignite2 = grid(2);

            assertNotNull(ignite2.services().service("testService"));
            assertNotNull(ignite2.services().service("testService2"));

            IgniteFuture undepFut = ignite4.services().cancelAsync("testService");
            IgniteFuture undepFut2 = ignite4.services().cancelAsync("testService2");

            IgniteEx ignite1 = grid(1);

            assertEquals(ignite1.localNode(), U.oldest(ignite4.cluster().nodes(), null));

            ignite1.close();

            undepFut.get(TEST_FUTURE_WAIT_TIMEOUT);
            undepFut2.get(TEST_FUTURE_WAIT_TIMEOUT);

            assertNull(ignite4.services().service("testService"));
            assertNull(ignite4.services().service("testService2"));
        }
        finally {
            stopAllGrids();
        }
    }
}
