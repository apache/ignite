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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;

/**
 * Test for {@link TcpDiscoverySpi} with Multi Data Centers where coordinator changed on second node join.
 */
public class TcpDiscoveryMdcSelfWithCoordinatorChangeTest extends TcpDiscoveryMdcSelfPlainTest {
    /**
     * @throws Exception If fails.
     */
    public TcpDiscoveryMdcSelfWithCoordinatorChangeTest() throws Exception {
    }

    @Override protected void applyDC(){
        String prev = System.getProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, prev == null ? DC_ID_1 : DC_ID_0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testDiscoveryEventsDiscard() throws Exception {
        try {
            // disable metrics to avoid sending metrics messages as they may mess with pending messages counting.
            metricsEnabled = false;

            TestEventDiscardSpi spi = new TestEventDiscardSpi();

            Ignite ignite0 = startGrid(0);

            nodeSpi.set(spi);

            startGrid(1);

            ignite0.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)); // Send custom message.

            ignite0.destroyCache(DEFAULT_CACHE_NAME); // Send custom message.

            // We need to wait for discartion of pending messages from asynchronous processes like removing cache
            // metrics from DMS. Initially the test was correct as createCache/destroyCache methods are synchronous
            // and block test-runner thread for long enough for pending messages to be discarded.
            // But at some point aforementioned operations were added and implicit assumption the test relies on was broken.
            boolean pendingMsgsDiscarded = waitPendingMessagesDiscarded(spi);
            assertTrue(pendingMsgsDiscarded);

            stopGrid(0);

            log.info("Start new node.");

            spi.checkDuplicates = true;

            startGrid(0);

            spi.checkDuplicates = false;

            assertFalse(spi.failed);
        }
        finally {
            stopAllGrids();

            metricsEnabled = true;
        }
    }
}
