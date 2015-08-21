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

import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.spi.*;

import java.io.*;
import java.util.*;

/**
 * Grid TCP discovery SPI start stop self test.
 */
@GridSpiTest(spi = TcpDiscoverySpi.class, group = "Discovery SPI")
public class TcpDiscoverySpiStartStopSelfTest extends GridSpiStartStopAbstractTest<TcpDiscoverySpi> {
    /**
     * @return IP finder.
     */
    @GridSpiTestConfig
    public TcpDiscoveryIpFinder getIpFinder() {
        return new TcpDiscoveryVmIpFinder(true);
    }

    /**
     * @return Discovery data collector.
     */
    @GridSpiTestConfig
    public DiscoverySpiDataExchange getDataExchange() {
        return new DiscoverySpiDataExchange() {
            @Override public Map<Integer, Serializable> collect(UUID nodeId) {
                return Collections.emptyMap();
            }

            @Override public void onExchange(UUID joiningNodeId, UUID nodeId, Map<Integer, Serializable> data) {
                // No-op.
            }
        };
    }
}
