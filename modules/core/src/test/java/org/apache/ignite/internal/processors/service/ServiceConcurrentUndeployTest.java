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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.DiscoverySpiTestListener;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.service.inner.LongInitializedTestService;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests concurrent deploy/undeploy services.
 */
public class ServiceConcurrentUndeployTest extends GridCommonAbstractTest {
    /** */
    private final CountDownLatch waitLatch = new CountDownLatch(2);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestTcpDiscoverySpi disco = new TestTcpDiscoverySpi();

        disco.setInternalListener(new DiscoverySpiTestListener() {
            @Override public boolean beforeSendCustomEvent(DiscoverySpi spi, IgniteLogger log, DiscoveryCustomMessage msg) {
                if (spi.isClientMode()) {
                    DiscoveryCustomMessage realMsg = GridTestUtils.unwrap(msg);

                    boolean isUndeployMsg = realMsg instanceof ServiceChangeBatchRequest;

                    if (isUndeployMsg) {
                        ServiceChangeBatchRequest batch = (ServiceChangeBatchRequest)realMsg;

                        long undeployReqCnt = batch.requests().stream()
                            .filter(r -> r instanceof ServiceUndeploymentRequest)
                            .count();

                        if (undeployReqCnt > 0) {
                            assertEquals(1, undeployReqCnt);
                            assertTrue(waitLatch.getCount() > 0);

                            waitLatch.countDown();

                            try {
                                assertTrue(waitLatch.await(1, TimeUnit.MINUTES));
                            }
                            catch (InterruptedException e) {
                                throw new IgniteException(e);
                            }
                        }
                    }
                }

                return super.beforeSendCustomEvent(spi, log, msg);
            }
        });

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** */
    @Test
    public void test() throws Exception {
        try (IgniteEx ignite = startGrid(0); IgniteEx client0 = startClientGrid(1); IgniteEx client1 = startClientGrid(2)) {
            client0.services().deployNodeSingletonAsync(
                "myservice",
                new LongInitializedTestService(ThreadLocalRandom.current().nextLong(1001))
            ).get(1, TimeUnit.MINUTES);

            // 1. Each client sees deployed service.
            // 2. Each client sends request to undeploy service.
            // 3. On second undeploy error throws.
            IgniteInternalFuture<Void> fut0 = runAsync(() -> client0.services().cancelAllAsync().get());
            IgniteInternalFuture<Void> fut1 = runAsync(() -> client1.services().cancelAllAsync().get());

            fut0.get(1, TimeUnit.MINUTES);
            fut1.get(1, TimeUnit.MINUTES);

            assertEquals(0, waitLatch.getCount());
        }
    }
}
