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

import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class ZookeeperDiscoverySpiSaslSuccessfulAuthTest extends ZookeeperDiscoverySpiSaslAuthAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteNodesWithValidPasswordSuccessfullyJoins() throws Exception {
        System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY,
            "ValidZookeeperClient");

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
