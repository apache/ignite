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

import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ZOOKEEPER_DISCOVERY_MAX_RETRY_COUNT;
import static org.apache.zookeeper.client.ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY;

/**
 *
 */
public class ZookeeperDiscoverySpiSaslFailedAuthTest extends ZookeeperDiscoverySpiSaslAuthAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = LOGIN_CONTEXT_NAME_KEY, value = "InvalidZookeeperClient")
    @WithSystemProperty(key = IGNITE_ZOOKEEPER_DISCOVERY_MAX_RETRY_COUNT, value = "1")
    public void testIgniteNodeWithInvalidPasswordFailsToJoin() throws Exception {
        try {
            startGrid(0);

            Assert.fail("Ignite node with invalid password should fail on join.");
        }
        catch (Exception ignored) {
            //ignored
        }
    }
}
