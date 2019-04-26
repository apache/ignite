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

package org.apache.ignite.monitoring;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.monitoring.HttpPullExposerSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class MonitoringSelfTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi());
        cfg.setMonitoringExposerSpi(new HttpPullExposerSpi());

        return cfg;
    }

    @Test
    public void testMonitoring() throws Exception {
        Ignite grid = grid(0);

        deployService(grid);

        executeTask(grid, "testBroadcast");
        executeTask(grid, "testBroadcast2");
        executeTask(grid, "anotherBroadcast");

        Thread.sleep(60_000L);
    }

    private void executeTask(Ignite grid, String name) {
        grid.compute().withName(name).broadcastAsync(() -> {
            System.out.println("MonitoringSelfTest.testMonitoring - 1");
            try {
                Thread.sleep(20_000L);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("MonitoringSelfTest.testMonitoring - 2");
        });
    }

    private void deployService(Ignite grid) {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName("testService");
        cfg.setMaxPerNodeCount(1);
        cfg.setService(new MyService());

        grid.services().deploy(cfg);
    }

    public static class MyService implements Service {
        @Override public void cancel(ServiceContext ctx) {
            System.out.println("MonitoringSelfTest.cancel");
        }

        @Override public void init(ServiceContext ctx) throws Exception {
            System.out.println("MonitoringSelfTest.init");
        }

        @Override public void execute(ServiceContext ctx) throws Exception {
            System.out.println("MonitoringSelfTest.execute");
        }
    }
}
