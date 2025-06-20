

package org.apache.ignite.console.demo;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.agent.IgniteClusterLauncher;
import org.apache.ignite.console.agent.handlers.DemoClusterHandler;
import org.apache.ignite.console.demo.service.DemoCachesLoadService;
import org.apache.ignite.console.demo.service.DemoComputeLoadService;
import org.apache.ignite.console.demo.service.DemoRandomCacheLoadService;
import org.apache.ignite.console.demo.service.DemoServiceClusterSingleton;
import org.apache.ignite.console.demo.service.DemoServiceKeyAffinity;
import org.apache.ignite.console.demo.service.DemoServiceMultipleInstances;
import org.apache.ignite.console.demo.service.DemoServiceNodeSingleton;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
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
    private static final int WAL_SEGMENTS = 5;

    /** WAL file segment size, 16MBytes. */
    private static final int WAL_SEGMENT_SZ = 16 * 1024 * 1024;


    /** */
    private static volatile String demoUrl;

    /**
     * Configure node.
     *
     * @param basePort Base port.
     * @param gridIdx Ignite instance name index.
     * @param client If {@code true} then start client node.
     * @return IgniteConfiguration
     */
    private static IgniteConfiguration igniteConfiguration(IgniteConfiguration cfg, int basePort, int gridIdx, boolean client)
        throws IgniteCheckedException {        

        cfg.setGridLogger(new Slf4jLogger());

        cfg.setIgniteInstanceName(DemoClusterHandler.DEMO_CLUSTER_NAME);
        cfg.setLocalHost("127.0.0.1");
        cfg.setEventStorageSpi(new MemoryEventStorageSpi());
        

        File workDir = new File(U.workDirectory(null, null), "demo-work");

        cfg.setWorkDirectory(workDir.getAbsolutePath());

        int[] evts = new int[EVTS_DISCOVERY.length];

        System.arraycopy(EVTS_DISCOVERY, 0, evts, 0, EVTS_DISCOVERY.length);
        

        cfg.setIncludeEventTypes(evts);

        cfg.getConnectorConfiguration().setPort(basePort + gridIdx);
        cfg.getConnectorConfiguration().setJettyPath("http://0.0.0.0:"+(basePort + 10 + gridIdx));

        if(cfg.getCommunicationSpi()!=null) {
            TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

            int discoPort = basePort + 20;

            ipFinder.setAddresses(Collections.singletonList("127.0.0.1:" + discoPort  + ".." + (discoPort + 10)));

            // Configure discovery SPI.
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.setLocalPort(discoPort);
            discoSpi.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(discoSpi);

            TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

            commSpi.setMessageQueueLimit(10);

            int commPort = basePort + 30;

            commSpi.setLocalPort(commPort);

            cfg.setCommunicationSpi(commSpi);
        }

        cfg.setGridLogger(new Slf4jLogger(log));
        cfg.setMetricsLogFrequency(0);

        DataRegionConfiguration dataRegCfg = new DataRegionConfiguration();
        dataRegCfg.setName("demo");
        dataRegCfg.setMetricsEnabled(true);
        dataRegCfg.setMaxSize(DFLT_DATA_REGION_INITIAL_SIZE);
        dataRegCfg.setPersistenceEnabled(false);
        dataRegCfg.setLazyMemoryAllocation(true);

        DataStorageConfiguration dataStorageCfg = new DataStorageConfiguration();
        dataStorageCfg.setMetricsEnabled(true);
        
        dataStorageCfg.setStoragePath("data");
        dataStorageCfg.setDefaultDataRegionConfiguration(dataRegCfg);
        dataStorageCfg.setSystemRegionMaxSize(DFLT_DATA_REGION_INITIAL_SIZE);

        dataStorageCfg.setWalMode(LOG_ONLY);
        dataStorageCfg.setWalSegments(WAL_SEGMENTS);
        dataStorageCfg.setWalSegmentSize(WAL_SEGMENT_SZ);

        cfg.setDataStorageConfiguration(dataStorageCfg);

        cfg.setClientMode(client);

        cfg.setFailureHandler(new StopNodeFailureHandler());
        cfg.setMetricsLogFrequency(0);

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
    public static String getDemoUrl() {
        return demoUrl;
    }

    public static Ignite tryStart(String clusterName, String cfgFile, int idx, boolean lastNode) throws IgniteCheckedException {
        File configWorkFile = new File(cfgFile);
        Ignite ignite = null;
        IgniteConfiguration icfg = new IgniteConfiguration();
        GridSpringResourceContext springCtx = null;
        if(configWorkFile.exists()) {
            IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgMap=null;
            if(ignite==null) {
                URL springPreCfgUrl = U.resolveSpringUrl(configWorkFile.toString());
                cfgMap = IgnitionEx.loadConfigurations(springPreCfgUrl);

                Collection<IgniteConfiguration> cfgList = cfgMap.get1();
                for(IgniteConfiguration cfg0: cfgList) {
                    if(clusterName.equals(cfg0.getIgniteInstanceName())){
                        icfg = cfg0;
                    }
                }
                springCtx = cfgMap.getValue();
            }
        }

        // 启动Demo节点，并且在最后一个节点部署服务
        ignite = AgentClusterDemo.tryStart(icfg,idx,lastNode,springCtx);
        return ignite;
    }

    /**
     * Start ignite node with cacheEmployee and populate it with data.
     */
    public static Ignite tryStart(IgniteConfiguration cfg, int idx, boolean lastNode,GridSpringResourceContext springCtx) {
        if (initGuard.compareAndSet(false, true)) {
            log.info("DEMO: Starting embedded nodes for demo...");

            System.setProperty(IGNITE_NO_ASCII, "true");
            System.setProperty(IGNITE_QUIET, "false");
            System.setProperty(IGNITE_UPDATE_NOTIFIER, "false");

            System.setProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, "20");
            System.setProperty(IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED, "true");
            System.setProperty(IGNITE_SQL_DISABLE_SYSTEM_VIEWS, "false");
            
            final AtomicInteger basePort = new AtomicInteger(60700);
           
            int port = basePort.get();
            Ignite ignite = null;

            try {
                igniteConfiguration(cfg, port, idx, false);

                if (lastNode) {
                    U.delete(Paths.get(cfg.getWorkDirectory()));

                    U.resolveWorkDirectory(
                        cfg.getWorkDirectory(),
                        cfg.getDataStorageConfiguration().getStoragePath(),
                        true
                    );          
                    cfg.setConsistentId(cfg.getIgniteInstanceName()+"_"+ idx);
                }
                else {                	
                	cfg.setClusterStateOnStart(ClusterState.INACTIVE);
                	cfg.setConsistentId(cfg.getIgniteInstanceName()+"_"+ idx);
                }

                ignite = IgnitionEx.start(cfg,springCtx);

                if (ignite!=null) {
                    demoUrl = IgniteClusterLauncher.getNodeRestUrl(ignite);;
                    log.info("Cluster: Started embedded node for data analysis purpose Jetty REST {}]", demoUrl);
                }                    
            }
            catch (Throwable e) {
                if (lastNode) {
                    basePort.getAndAdd(50);

                    log.warn("DEMO: Failed to start embedded node.", e);
                }
                else
                    log.error("DEMO: Failed to start embedded node.", e);
            }
            finally {
                if (lastNode && ignite != null) {
                    try {    
                    	Thread.sleep(1000*idx);
                    	while(ignite.cluster().nodes().size()<idx) {
                    		Thread.sleep(1000);
                    	}                       
                    	ignite.cluster().state(ClusterState.ACTIVE);

                        deployServices(ignite.services(ignite.cluster().forServers()));

                        log.info("DEMO: All embedded nodes for demo successfully started");
                    }
                    catch (Throwable ignored) {
                        log.info("DEMO: Failed to launch demo load");
                    }                    
                }
            }
            return ignite;
        }
        return null;
    }

    /** */
    public static void stop() {
        demoUrl = null;

        Ignition.stop(DemoClusterHandler.DEMO_CLUSTER_NAME,true);
        
        initGuard.compareAndSet(true, false);
    }
}
