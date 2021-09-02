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

package org.apache.ignite.spi.discovery.zk.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;

/**
 * Tests for Zookeeper SPI discovery.
 */
public class ZookeeperDiscoveryCustomEventsTest extends ZookeeperDiscoverySpiTestBase {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomEventsSimple1_SingleNode() throws Exception {
        ZookeeperDiscoverySpiTestHelper.ackEveryEventSystemProperty();

        Ignite srv0 = startGrid(0);

        srv0.createCache(new CacheConfiguration<>("c1"));

        helper.waitForEventsAcks(srv0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCustomEventsSimple1_5_Nodes() throws Exception {
        ZookeeperDiscoverySpiTestHelper.ackEveryEventSystemProperty();

        Ignite srv0 = startGrids(5);

        srv0.createCache(new CacheConfiguration<>("c1"));

        awaitPartitionMapExchange();

        helper.waitForEventsAcks(srv0);
    }
}
