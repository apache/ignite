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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.curator.test.ByteCodeRewrite;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperClientTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiSaslFailedAuthTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiSaslSuccessfulAuthTest;
import org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoverySpiTest;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.quorum.LearnerZooKeeperServer;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 *
 */
@RunWith(AllTests.class)
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

    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        System.setProperty("zookeeper.forceSync", "false");
        System.setProperty("zookeeper.jmx.log4j.disable", "true");

        TestSuite suite = new TestSuite("ZookeeperDiscoverySpi Test Suite");

        suite.addTest(new JUnit4TestAdapter(ZookeeperClientTest.class));
        suite.addTest(new JUnit4TestAdapter(ZookeeperDiscoverySpiTest.class));
        suite.addTest(new JUnit4TestAdapter(ZookeeperDiscoverySpiSaslFailedAuthTest.class));
        suite.addTest(new JUnit4TestAdapter(ZookeeperDiscoverySpiSaslSuccessfulAuthTest.class));

        return suite;
    }
}
