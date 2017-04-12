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

package org.apache.ignite.internal.processors.compute;

import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ExecutorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests custom executor named pools.
 *
 * https://issues.apache.org/jira/browse/IGNITE-4699
 */
public class IgniteComputeCustomExecutorConfigTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConfigurations() throws Exception {
        try {
            checkStartWithInvalidCfg(getConfiguration("node0")
                .setExecutorConfiguration(new ExecutorConfiguration("")), "Executor name must not be empty");

            checkStartWithInvalidCfg(getConfiguration("node0")
                .setExecutorConfiguration(new ExecutorConfiguration("exec"),
                    new ExecutorConfiguration("exec")), "Executor name 'exec' is duplicated");

            checkStartWithInvalidCfg(getConfiguration("node0")
                    .setExecutorConfiguration(new ExecutorConfiguration("exec").setSize(-1)),
                "Invalid exec thread pool size");

            checkStartWithInvalidCfg(getConfiguration("node0")
                    .setExecutorConfiguration(new ExecutorConfiguration("exec").setSize(0)),
                "Invalid exec thread pool size");
        }
        finally {
            Ignition.stopAll(true);
        }
    }

    /**
     * @param cfg Ignite configuration.
     * @param msg Exception message pattern.
     * @throws Exception If failed.
     */
    private void checkStartWithInvalidCfg(IgniteConfiguration cfg, String msg) throws Exception {
        try {
            Ignition.start(cfg);

            fail("Creation must be failed");
        }
        catch (IgniteException e) {
            assertTrue("Not expected exception" + e.getMessage(), e.getMessage()
                .contains(msg));
        }
    }
}
