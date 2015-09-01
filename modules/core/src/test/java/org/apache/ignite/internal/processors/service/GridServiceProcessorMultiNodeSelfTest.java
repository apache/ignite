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

import java.util.concurrent.CountDownLatch;
import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Single node services test.
 */
public class GridServiceProcessorMultiNodeSelfTest extends GridServiceProcessorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 4;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingletonUpdateTopology() throws Exception {
        String name = "serviceSingletonUpdateTopology";

        Ignite g = randomGrid();

        CountDownLatch latch = new CountDownLatch(1);

        DummyService.exeLatch(name, latch);

        IgniteServices svcs = g.services().withAsync();

        svcs.deployClusterSingleton(name, new DummyService());

        IgniteFuture<?> fut = svcs.future();

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        TestCase.assertEquals(name, 1, DummyService.started(name));
        TestCase.assertEquals(name, 0, DummyService.cancelled(name));

        int nodeCnt = 2;

        startExtraNodes(nodeCnt);

        try {
            TestCase.assertEquals(name, 1, DummyService.started(name));
            TestCase.assertEquals(name, 0, DummyService.cancelled(name));

            info(">>> Passed checks.");

            checkCount(name, g.services().serviceDescriptors(), 1);
        }
        finally {
            stopExtraNodes(nodeCnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityDeployUpdateTopology() throws Exception {
        Ignite g = randomGrid();

        final Integer affKey = 1;

        // Store a cache key.
        g.cache(CACHE_NAME).put(affKey, affKey.toString());

        String name = "serviceAffinityUpdateTopology";

        IgniteServices svcs = g.services().withAsync();

        svcs.deployKeyAffinitySingleton(name, new AffinityService(affKey),
            CACHE_NAME, affKey);

        IgniteFuture<?> fut = svcs.future();

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        checkCount(name, g.services().serviceDescriptors(), 1);

        int nodeCnt = 2;

        startExtraNodes(nodeCnt);

        try {
            checkCount(name, g.services().serviceDescriptors(), 1);
        }
        finally {
            stopExtraNodes(nodeCnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployOnEachNodeUpdateTopology() throws Exception {
        String name = "serviceOnEachNodeUpdateTopology";

        Ignite g = randomGrid();

        CountDownLatch latch = new CountDownLatch(nodeCount());

        DummyService.exeLatch(name, latch);

        IgniteServices svcs = g.services().withAsync();

        svcs.deployNodeSingleton(name, new DummyService());

        IgniteFuture<?> fut = svcs.future();

        info("Deployed service: " + name);

        fut.get();

        info("Finished waiting for service future: " + name);

        latch.await();

        TestCase.assertEquals(name, nodeCount(), DummyService.started(name));
        TestCase.assertEquals(name, 0, DummyService.cancelled(name));

        int newNodes = 2;

        latch = new CountDownLatch(newNodes);

        DummyService.exeLatch(name, latch);

        startExtraNodes(newNodes);

        try {
            latch.await();

            TestCase.assertEquals(name, nodeCount() + newNodes, DummyService.started(name));
            TestCase.assertEquals(name, 0, DummyService.cancelled(name));

            checkCount(name, g.services().serviceDescriptors(), nodeCount() + newNodes);
        }
        finally {
            stopExtraNodes(newNodes);
        }
    }
}