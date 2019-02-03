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

import java.io.File;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.demo.service.DemoCachesLoadService;
import org.apache.ignite.console.demo.service.DemoComputeLoadService;
import org.apache.ignite.console.demo.service.DemoRandomCacheLoadService;
import org.apache.ignite.console.demo.service.DemoServiceClusterSingleton;
import org.apache.ignite.console.demo.service.DemoServiceKeyAffinity;
import org.apache.ignite.console.demo.service.DemoServiceMultipleInstances;
import org.apache.ignite.console.demo.service.DemoServiceNodeSingleton;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_NO_ASCII;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.console.demo.AgentDemoUtils.newScheduledThreadPool;
import static org.apache.ignite.events.EventType.EVTS_DISCOVERY;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_ADDRS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_PORT;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.VISOR_TASK_EVTS;

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
    private static final String SRV_NODE_NAME = "demo-server-";

    /** */
    private static final String CLN_NODE_NAME = "demo-client-";

    /** */
    private static final int NODE_CNT = 3;

    /** */
    private static final int WAL_SEGMENTS = 5;

    /** WAL file segment size, 16MBytes. */
    private static final int WAL_SEGMENT_SZ = 16 * 1024 * 1024;

    /** */
    private static CountDownLatch initLatch = new CountDownLatch(1);

    /** */
    private static volatile List<String> demoUrl;

    /**
     * Configure node.
     *
     * @param basePort Base port.
     * @param gridIdx Ignite instance name index.
     * @param client If {@code true} then start client node.
     * @return IgniteConfiguration
     */
    private static IgniteConfiguration igniteConfiguration(int basePort, int gridIdx, boolean client)
        throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridLogger(new Slf4jLogger());

        cfg.setIgniteInstanceName((client ? CLN_NODE_NAME : SRV_NODE_NAME) + gridIdx);
        cfg.setLocalHost("127.0.0.1");
        cfg.setEventStorageSpi(new MemoryEventStorageSpi());
        cfg.setConsistentId(cfg.getIgniteInstanceName());

        File workDir = new File(U.workDirectory(null, null), "demo-work");

        cfg.setWorkDirectory(workDir.getAbsolutePath());

        int[] evts = new int[EVTS_DISCOVERY.length + VISOR_TASK_EVTS.length];

        System.arraycopy(EVTS_DISCOVERY, 0, evts, 0, EVTS_DISCOVERY.length);
        System.arraycopy(VISOR_TASK_EVTS, 0, evts, EVTS_DISCOVERY.length, VISOR_TASK_EVTS.length);

        cfg.setIncludeEventTypes(evts);

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

        DataRegionConfiguration dataRegCfg = new DataRegionConfiguration();
        dataRegCfg.setName("demo");
        dataRegCfg.setMetricsEnabled(true);
        dataRegCfg.setMaxSize(DFLT_DATA_REGION_INITIAL_SIZE);
        dataRegCfg.setPersistenceEnabled(true);

        DataStorageConfiguration dataStorageCfg = new DataStorageConfiguration();
        dataStorageCfg.setMetricsEnabled(true);
        dataStorageCfg.setStoragePath(PdsConsistentIdProcessor.DB_DEFAULT_FOLDER);
        dataStorageCfg.setDefaultDataRegionConfiguration(dataRegCfg);
        dataStorageCfg.setSystemRegionMaxSize(DFLT_DATA_REGION_INITIAL_SIZE);

        dataStorageCfg.setWalMode(LOG_ONLY);
        dataStorageCfg.setWalSegments(WAL_SEGMENTS);
        dataStorageCfg.setWalSegmentSize(WAL_SEGMENT_SZ);

        cfg.setDataStorageConfiguration(dataStorageCfg);

        cfg.setClientMode(client);

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
        services.deployClusterSingleton("Demo caches load service", new DemoCachesLoadService(20));
        services.deployKeyAffinitySingleton("Demo service: Key affinity singleton",
            new DemoServiceKeyAffinity(), DemoCachesLoadService.CAR_CACHE_NAME, "id");

        services.deployNodeSingleton("RandomCache load service", new DemoRandomCacheLoadService(20));

        services.deployMultiple("Demo service: Compute load", new DemoComputeLoadService(), 2, 1);
    }

    /** */
    public static List<String> getDemoUrl() {
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

            System.setProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, "20");
            System.setProperty(IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED, "true");

            final AtomicInteger basePort = new AtomicInteger(60700);
            final AtomicInteger cnt = new AtomicInteger(-1);

            final ScheduledExecutorService execSrv = newScheduledThreadPool(1, "demo-nodes-start");

            execSrv.scheduleAtFixedRate(new Runnable() {
                @Override public void run() {
                    int idx = cnt.incrementAndGet();
                    int port = basePort.get();

                    boolean first = idx == 0;

                    try {
                        IgniteConfiguration cfg = igniteConfiguration(port, idx, false);

                        if (first) {
                            U.delete(Paths.get(cfg.getWorkDirectory()));

                            U.resolveWorkDirectory(
                                cfg.getWorkDirectory(),
                                cfg.getDataStorageConfiguration().getStoragePath(),
                                true
                            );
                        }

                        Ignite ignite = Ignition.start(cfg);

                        if (first) {
                            ClusterNode node = ignite.cluster().localNode();

                            Collection<String> jettyAddrs = node.attribute(ATTR_REST_JETTY_ADDRS);

                            if (jettyAddrs == null) {
                                Ignition.stopAll(true);

                                throw new IgniteException("DEMO: Failed to start Jetty REST server on embedded node");
                            }

                            String jettyHost = jettyAddrs.iterator().next();

                            Integer jettyPort = node.attribute(ATTR_REST_JETTY_PORT);

                            if (F.isEmpty(jettyHost) || jettyPort == null)
                                throw new IgniteException("DEMO: Failed to start Jetty REST handler on embedded node");

                            log.info("DEMO: Started embedded node for demo purpose [TCP binary port={}, Jetty REST port={}]", port, jettyPort);

                            demoUrl = Collections.singletonList(String.format("http://%s:%d", jettyHost, jettyPort));

                            initLatch.countDown();
                        }
                    }
                    catch (Throwable e) {
                        if (first) {
                            basePort.getAndAdd(50);

                            log.warn("DEMO: Failed to start embedded node.", e);
                        }
                        else
                            log.error("DEMO: Failed to start embedded node.", e);
                    }
                    finally {
                        if (idx == NODE_CNT) {
                            Ignite ignite = Ignition.ignite(SRV_NODE_NAME + 0);

                            if (ignite != null) {
                                ignite.cluster().active(true);

                                deployServices(ignite.services(ignite.cluster().forServers()));
                            }

                            log.info("DEMO: All embedded nodes for demo successfully started");

                            execSrv.shutdown();
                        }
                    }
                }
            }, 1, 5, TimeUnit.SECONDS);
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
