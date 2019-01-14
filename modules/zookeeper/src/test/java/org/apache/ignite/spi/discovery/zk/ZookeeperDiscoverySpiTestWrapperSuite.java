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
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiTest2;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiTest3;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiTest4;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiTest5;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiTest6;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiTest7;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiTest8;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.LearnerZooKeeperServer;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/** */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//todo restore after testing:     ZookeeperDiscoverySpiTest2.class,
//todo restore after testing:     ZookeeperDiscoverySpiTest3.class,
    ZookeeperDiscoverySpiTest4.class,
    //todo restore after testing: ZookeeperDiscoverySpiTest5.class,
    //todo restore after testing: ZookeeperDiscoverySpiTest6.class,
    //todo restore after testing: ZookeeperDiscoverySpiTest7.class,
    //todo restore after testing: ZookeeperDiscoverySpiTest8.class,
    //todo restore after testing: ZookeeperDiscoverySpiTest.class,
})
public class ZookeeperDiscoverySpiTestWrapperSuite {
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
