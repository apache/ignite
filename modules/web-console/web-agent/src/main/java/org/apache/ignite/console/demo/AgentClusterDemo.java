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

package org.apache.ignite.console.demo;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.demo.service.DemoCachesLoadService;
import org.apache.ignite.console.demo.service.DemoRandomCacheLoadService;
import org.apache.ignite.console.demo.service.DemoServiceMultipleInstances;
import org.apache.ignite.console.demo.service.DemoServiceClusterSingleton;
import org.apache.ignite.console.demo.service.DemoServiceKeyAffinity;
import org.apache.ignite.console.demo.service.DemoServiceNodeSingleton;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.log4j.Logger;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_ASCII;
import static org.apache.ignite.events.EventType.EVTS_DISCOVERY;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_ADDRS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_PORT;

/**
 * Demo for cluster features like SQL and Monitoring.
 *
 * Cache will be created and populated with data to query.
 */
public class AgentClusterDemo {
    /** */
    private static final Logger log = Logger.getLogger(AgentClusterDemo.class.getName());

    /** */
    private static final AtomicBoolean initLatch = new AtomicBoolean();

    /** */
    private static final int NODE_CNT = 3;

    /**
     * Configure node.
     * @param gridIdx Grid name index.
     * @param client If {@code true} then start client node.
     * @return IgniteConfiguration
     */
    private static  IgniteConfiguration igniteConfiguration(int gridIdx, boolean client) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName((client ? "demo-client-" : "demo-server-" ) + gridIdx);
        cfg.setLocalHost("127.0.0.1");
        cfg.setIncludeEventTypes(EVTS_DISCOVERY);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:60900.." + (60900 + NODE_CNT - 1)));

        // Configure discovery SPI.
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setLocalPort(60900);
        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);
        commSpi.setLocalPort(60800);

        cfg.setCommunicationSpi(commSpi);
        cfg.setGridLogger(new Log4JLogger(log));
        cfg.setMetricsLogFrequency(0);
        cfg.getConnectorConfiguration().setPort(60700);

        if (client)
            cfg.setClientMode(true);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return cfg;
    }

    /**
     * Starts read and write from cache in background.
     *
     * @param ignite Ignite.
     * @param cnt - maximum count read/write key
     */
    private static void startLoad(final Ignite ignite, final int cnt) {
        ignite.services().deployClusterSingleton("Demo caches load service", new DemoCachesLoadService(cnt));
        ignite.services().deployNodeSingleton("RandomCache load service", new DemoRandomCacheLoadService(cnt));
    }

    /**
     * Start ignite node with cacheEmployee and populate it with data.
     */
    public static boolean testDrive(AgentConfiguration acfg) {
        if (initLatch.compareAndSet(false, true)) {
            log.info("DEMO: Starting embedded nodes for demo...");

            System.setProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, "1");
            System.setProperty(IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED, "true");
            System.setProperty(IGNITE_UPDATE_NOTIFIER, "false");

            System.setProperty(IGNITE_JETTY_PORT, "60800");
            System.setProperty(IGNITE_NO_ASCII, "true");

            try {
                IgniteEx ignite = (IgniteEx)Ignition.start(igniteConfiguration(0, false));

                final AtomicInteger cnt = new AtomicInteger(0);

                final ScheduledExecutorService execSrv = Executors.newSingleThreadScheduledExecutor();

                execSrv.scheduleAtFixedRate(new Runnable() {
                    @Override public void run() {
                        int idx = cnt.incrementAndGet();

                        try {
                            Ignition.start(igniteConfiguration(idx, idx == NODE_CNT));
                        }
                        catch (Throwable e) {
                            log.error("DEMO: Failed to start embedded node: " + e.getMessage());
                        }
                        finally {
                            if (idx == NODE_CNT)
                                execSrv.shutdown();
                        }
                    }
                }, 10, 10, TimeUnit.SECONDS);

                IgniteServices services = ignite.services();

                services.deployMultiple("Demo service: Multiple instances", new DemoServiceMultipleInstances(), 7, 3);
                services.deployNodeSingleton("Demo service: Node singleton", new DemoServiceNodeSingleton());
                services.deployClusterSingleton("Demo service: Cluster singleton", new DemoServiceClusterSingleton());
                services.deployKeyAffinitySingleton("Demo service: Key affinity singleton",
                    new DemoServiceKeyAffinity(), DemoCachesLoadService.CAR_CACHE_NAME, "id");

                if (log.isDebugEnabled())
                    log.debug("DEMO: Started embedded nodes with indexed enabled caches...");

                Collection<String> jettyAddrs = ignite.localNode().attribute(ATTR_REST_JETTY_ADDRS);

                String host = jettyAddrs == null ? null : jettyAddrs.iterator().next();

                Integer port = ignite.localNode().attribute(ATTR_REST_JETTY_PORT);

                if (F.isEmpty(host) || port == null) {
                    log.error("DEMO: Failed to start embedded node with rest!");

                    return false;
                }

                acfg.demoNodeUri(String.format("http://%s:%d", host, port));

                log.info("DEMO: Embedded nodes for sql and monitoring demo successfully started");

                startLoad(ignite, 20);
            }
            catch (Exception e) {
                log.error("DEMO: Failed to start embedded node for sql and monitoring demo!", e);

                return false;
            }
        }

        return true;
    }
}
