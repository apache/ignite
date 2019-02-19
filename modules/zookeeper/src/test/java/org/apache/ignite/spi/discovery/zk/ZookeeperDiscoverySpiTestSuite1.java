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

package org.apache.ignite.spi.discovery.zk;

import org.apache.curator.test.ByteCodeRewrite;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperClientTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryClientReconnectTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiSaslFailedAuthTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiSaslSuccessfulAuthTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryMiscTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySegmentationAndConnectionRestoreTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryConcurrentStartAndStartStopTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryTopologyChangeAndReconnectTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryCommunicationFailureTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryClientDisconnectTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySplitBrainTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryCustomEventsTest;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.LearnerZooKeeperServer;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ZookeeperDiscoverySegmentationAndConnectionRestoreTest.class,
    ZookeeperDiscoveryConcurrentStartAndStartStopTest.class,
    ZookeeperDiscoveryTopologyChangeAndReconnectTest.class,
    ZookeeperDiscoveryCommunicationFailureTest.class,
    ZookeeperDiscoveryClientDisconnectTest.class,
    ZookeeperDiscoveryClientReconnectTest.class,
    ZookeeperDiscoverySplitBrainTest.class,
    ZookeeperDiscoveryCustomEventsTest.class,
    ZookeeperDiscoveryMiscTest.class,
    ZookeeperClientTest.class,
    ZookeeperDiscoverySpiSaslFailedAuthTest.class,
    ZookeeperDiscoverySpiSaslSuccessfulAuthTest.class,
})
public class ZookeeperDiscoverySpiTestSuite1 {
    /**
     * During test suite processing GC can unload some classes whose bytecode has been rewritten here
     * {@link ByteCodeRewrite}. And the next time these classes will be loaded without bytecode rewriting.
     *
     * This workaround prevents unloading of these classes.
     *
     * @see <a href="https://github.com/Netflix/curator/issues/121">Issue link.</a>.
     */
    @SuppressWarnings("unused")
    private static final Class[] WORKAROUND;

    static {
        ByteCodeRewrite.apply();

        // GC will not unload this classes.
        WORKAROUND = new Class[] {ZooKeeperServer.class, LearnerZooKeeperServer.class, MBeanRegistry.class};
    }

    /** */
    @BeforeClass
    public static void init() {
        System.setProperty("zookeeper.forceSync", "false");
        System.setProperty("zookeeper.jmx.log4j.disable", "true");
    }
}
