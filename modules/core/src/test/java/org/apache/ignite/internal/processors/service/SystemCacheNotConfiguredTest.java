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

package org.apache.ignite.internal.processors.service;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests if system cache was started before deploying of service.
 */
public class SystemCacheNotConfiguredTest extends GridCommonAbstractTest {
    /** */
    private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private final PrintStream originalErr = System.err;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        discoverySpi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discoverySpi);

        if("server".equals(igniteInstanceName))
            cfg.setServiceConfiguration(serviceConfiguration());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        captureErr();

        new Thread(this::startServer).start();

        Ignite client = startGrid(getConfiguration("client").setClientMode(true));

        IgniteServices services = client.services();

        SimpleService srvc = services.serviceProxy("service", SimpleService.class, false);

        Thread.sleep(1000);

        srvc.isWorking();

        assertFalse(getErr().contains("Cache is not configured:"));
    }

    /**
     * Start server node.
     */
    private void startServer() {
        try {
            startGrid(getConfiguration("server"));
        }
        catch (Exception e) {
            fail();
        }
    }

    /**
     * @return Service configuration.
     */
    private ServiceConfiguration serviceConfiguration() {
        ServiceConfiguration svcCfg = new ServiceConfiguration();

        svcCfg.setName("service");
        svcCfg.setTotalCount(1);
        svcCfg.setService(new SimpleServiceImpl());

        return svcCfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        System.setErr(originalErr);
    }

    /**
     * Turns on stdErr output capture.
     */
    private void captureErr() {
        System.setErr(new PrintStream(errContent));
    }

    /**
     * Turns off stdErr capture and returns the contents that have been captured.
     *
     * @return String of captured stdErr.
     */
    private String getErr() {
        return errContent.toString().replaceAll("\r", "");
    }

    /**
     * Simple service implementation for test.
     */
    public static class SimpleServiceImpl implements Service, SimpleService {
        /** {@inheritDoc} */
        SimpleServiceImpl() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void isWorking() {
            // No-op.
        }
    }

    /**
     * Simple service interface for test.
     */
    public interface SimpleService {
        /** */
        void isWorking();
    }
}
