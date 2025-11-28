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

import org.apache.ignite.IgniteSystemProperties;
import org.junit.Test;

/**
 *
 */
public class TcpDiscoveryMdcWithCoordinatorChangePendingMessageDeliveryTest extends TcpDiscoveryMdcPlainPendingMessageDeliveryTest {
    /**
     * @throws Exception If fails.
     */
    public TcpDiscoveryMdcWithCoordinatorChangePendingMessageDeliveryTest() throws Exception {
    }

    /** */
    @Override protected void applyDC() {
        String prev = System.getProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, prev == null ? DC_ID_1 : DC_ID_0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testPendingMessagesOverflow() throws Exception {
        startGrid(0);

        super.testPendingMessagesOverflow();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testCustomMessageInSingletonCluster() throws Exception {
        startGrid(0);

        super.testCustomMessageInSingletonCluster();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testDeliveryAllFailedMessagesInCorrectOrder() throws Exception {
        startGrid(0);

        super.testDeliveryAllFailedMessagesInCorrectOrder();
    }
}

