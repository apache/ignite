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

import org.apache.ignite.testframework.junits.spi.*;

/**
 *
 */
@GridSpiTest(spi = TcpClientDiscoverySpi.class, group = "Discovery SPI")
public class TcpClientDiscoverySpiConfigSelfTest extends GridSpiAbstractConfigTest<TcpDiscoverySpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new TcpClientDiscoverySpi(), "ipFinder", null);
        checkNegativeSpiProperty(new TcpClientDiscoverySpi(), "networkTimeout", 0);
        checkNegativeSpiProperty(new TcpClientDiscoverySpi(), "socketTimeout", 0);
        checkNegativeSpiProperty(new TcpClientDiscoverySpi(), "ackTimeout", 0);
        checkNegativeSpiProperty(new TcpClientDiscoverySpi(), "heartbeatFrequency", 0);
        checkNegativeSpiProperty(new TcpClientDiscoverySpi(), "threadPriority", -1);
        checkNegativeSpiProperty(new TcpClientDiscoverySpi(), "joinTimeout", -1);
    }
}
