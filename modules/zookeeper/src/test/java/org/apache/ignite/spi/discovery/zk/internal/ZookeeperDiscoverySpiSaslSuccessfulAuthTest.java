/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.spi.discovery.zk.internal;

import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.zookeeper.client.ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY;

/**
 *
 */
public class ZookeeperDiscoverySpiSaslSuccessfulAuthTest extends ZookeeperDiscoverySpiSaslAuthAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = LOGIN_CONTEXT_NAME_KEY, value = "ValidZookeeperClient")
    public void testIgniteNodesWithValidPasswordSuccessfullyJoins() throws Exception {
        startGrids(3);

        waitForTopology(3);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteNodeWithoutSaslConfigurationSuccessfullyJoins() throws Exception {
        //clearing SASL-related system properties that were set in beforeTest
        clearSaslSystemProperties();

        startGrid(0);

        waitForTopology(1);
    }
}
