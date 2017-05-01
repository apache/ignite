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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.demo.service.DemoCachesLoadService;
import org.apache.ignite.console.demo.service.DemoRandomCacheLoadService;
import org.apache.ignite.console.demo.service.DemoServiceClusterSingleton;
import org.apache.ignite.console.demo.service.DemoServiceKeyAffinity;
import org.apache.ignite.console.demo.service.DemoServiceMultipleInstances;
import org.apache.ignite.console.demo.service.DemoServiceNodeSingleton;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_ASCII;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;
import static org.apache.ignite.console.demo.AgentDemoUtils.newScheduledThreadPool;
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
    private static final Logger log = LoggerFactory.getLogger(AgentClusterDemo.class);

    /** */
    private static final AtomicBoolean initGuard = new AtomicBoolean();

    /** */
    private static CountDownLatch initLatch = new CountDownLatch(1);

    /** */
    private static volatile String demoUrl;

    /** */
    private static final int NODE_CNT = 3;

    /**
     * Configure node.
     * @param basePort Base port.
     * @param gridIdx Ignite instance name index.
     * @param client If {@code true} then start client node.
     * @return IgniteConfiguration
     */
    private static IgniteConfiguration igniteConfiguration(int basePort, int gridIdx, boolean client) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName((client ? "demo-client-" : "demo-server-" ) + gridIdx);
        cfg.setLocalHost("127.0.0.1");
        cfg.setEventStorageSpi(new MemoryEventStorageSpi());
        cfg.setIncludeEventTypes(EVTS_DISCOVERY);

        cfg.getConnectorConfiguration().setPort(basePort);

        System.setProperty(IGNITE_JETTY_PORT, String.valueOf(basePort + 10));

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        int discoPort = basePort + 20;

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:" + discoPort  + ".." + (discoPort + NODE_CNT - 1)));

        // Configure discovery SPI.
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setLocalPort(discoPort);
        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);
        commSpi.setMessageQueueLimit(10);

        int commPort = basePort + 30;

        commSpi.setLocalPort(commPort);

        cfg.setCommunicationSpi(commSpi);
        cfg.setGridLogger(new Slf4jLogger(log));
        cfg.setMetricsLogFrequency(0);

        if (client)
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * Starts read and write from cache in background.
     *
     * @param services Distributed services on the grid.
     */
    private static void deployServices(IgniteServices services) {
        services.deployMultiple("Demo service: Multiple instances", new DemoServiceMultipleInstances(), 7, 3);
        services.deployNodeSingleton("Demo service: Node singleton", new DemoServiceNodeSingleton());
        services.deployClusterSingleton("Demo service: Cluster singleton", new DemoServiceClusterSingleton());
        services.deployKeyAffinitySingleton("Demo service: Key affinity singleton",
            new DemoServiceKeyAffinity(), DemoCachesLoadService.CAR_CACHE_NAME, "id");

        services.deployClusterSingleton("Demo caches load service", new DemoCachesLoadService(20));
        services.deployNodeSingleton("RandomCache load service", new DemoRandomCacheLoadService(20));
    }

    /** */
    public static String getDemoUrl() {
        return demoUrl;
    }

    /**
     * Start ignite node with cacheEmployee and populate it with data.
     */
    public static CountDownLatch tryStart() {
        if (initGuard.compareAndSet(false, true)) {
            log.info("DEMO: Starting embedded nodes for demo...");

            System.setProperty(IGNITE_NO_ASCII, "true");
            System.setProperty(IGNITE_QUIET, "false");
            System.setProperty(IGNITE_UPDATE_NOTIFIER, "false");

            System.setProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, "1");
            System.setProperty(IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED, "true");

            final AtomicInteger basePort = new AtomicInteger(60700);
            final AtomicInteger cnt = new AtomicInteger(-1);

            final ScheduledExecutorService execSrv = newScheduledThreadPool(1, "demo-nodes-start");

            execSrv.scheduleAtFixedRate(new Runnable() {
                @Override public void run() {
                    int idx = cnt.incrementAndGet();
                    int port = basePort.get();

                    try {
                        IgniteEx ignite = (IgniteEx)Ignition.start(igniteConfiguration(port, idx, idx == NODE_CNT));

                        if (idx == 0) {
                            Collection<String> jettyAddrs = ignite.localNode().attribute(ATTR_REST_JETTY_ADDRS);

                            if (jettyAddrs == null) {
                                ignite.cluster().stopNodes();

                                throw new IgniteException("DEMO: Failed to start Jetty REST server on embedded node");
                            }

                            String jettyHost = jettyAddrs.iterator().next();

                            Integer jettyPort = ignite.localNode().attribute(ATTR_REST_JETTY_PORT);

                            if (F.isEmpty(jettyHost) || jettyPort == null)
                                throw new IgniteException("DEMO: Failed to start Jetty REST handler on embedded node");

                            log.info("DEMO: Started embedded node for demo purpose [TCP binary port={}, Jetty REST port={}]", port, jettyPort);

                            demoUrl = String.format("http://%s:%d", jettyHost, jettyPort);

                            initLatch.countDown();

                            deployServices(ignite.services());
                        }
                    }
                    catch (Throwable e) {
                        if (idx == 0) {
                            basePort.getAndAdd(50);

                            log.warn("DEMO: Failed to start embedded node.", e);
                        }
                        else
                            log.error("DEMO: Failed to start embedded node.", e);
                    }
                    finally {
                        if (idx == NODE_CNT) {
                            log.info("DEMO: All embedded nodes for demo successfully started");

                            execSrv.shutdown();
                        }
                    }
                }
            }, 1, 10, TimeUnit.SECONDS);
        }

        return initLatch;
    }

    /** */
    public static void stop() {
        demoUrl = null;

        Ignition.stopAll(true);

        initLatch = new CountDownLatch(1);

        initGuard.compareAndSet(true, false);
    }
}
