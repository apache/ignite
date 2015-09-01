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

import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;

/**
 *
 */
@GridSpiTest(spi = TcpDiscoverySpi.class, group = "Discovery SPI")
public class TcpDiscoverySpiConfigSelfTest extends GridSpiAbstractConfigTest<TcpDiscoverySpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "ipFinder", null);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "ipFinderCleanFrequency", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "localPort", 1023);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "localPortRange", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "networkTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "socketTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "ackTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "maxAckTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "reconnectCount", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "heartbeatFrequency", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "threadPriority", -1);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "maxMissedHeartbeats", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "statisticsPrintFrequency", 0);
    }
}