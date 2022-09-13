/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.agent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Base64;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterStartNodeResult;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.agent.handlers.ClusterHandler;
import org.apache.ignite.console.agent.service.CacheLoadDataService;
import org.apache.ignite.console.agent.service.ComputeTaskLoadService;
import org.apache.ignite.console.agent.service.CacheCopyDataService;
import org.apache.ignite.console.agent.service.ClusterAgentServiceList;
import org.apache.ignite.console.agent.service.CacheClearDataService;

import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.utils.Utils;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.isolated.IsolatedDiscoverySpi;
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
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;

import static org.apache.ignite.events.EventType.EVTS_DISCOVERY;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_ADDRS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_PORT;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.VISOR_TASK_EVTS;

/**
 * Launcher for cluster features like SQL and Monitoring.
 *
 * Cache will be created and populated with data to query.
 */
public class AgentClusterLauncher {
    /** */
    private static final Logger log = LoggerFactory.getLogger(AgentClusterLauncher.class);

    /** */
    private static final AtomicBoolean initGuard = new AtomicBoolean();

    private final static AtomicInteger basePort = new AtomicInteger(20700);

    /** */
    private static final int WAL_SEGMENTS = 5;

    /** WAL file segment size, 16MBytes. */
    private static final int WAL_SEGMENT_SZ = 16 * 1024 * 1024;

    /** */
    private static CountDownLatch initLatch = new CountDownLatch(1);
    

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
        cfg.setLocalHost("127.0.0.1");
        cfg.setEventStorageSpi(new MemoryEventStorageSpi());
        cfg.setConsistentId(cfg.getIgniteInstanceName());

        File workDir = new File(U.workDirectory(null, null), "launcher-work");

        cfg.setWorkDirectory(workDir.getAbsolutePath());

        int[] evts = new int[EVTS_DISCOVERY.length + VISOR_TASK_EVTS.length];

        System.arraycopy(EVTS_DISCOVERY, 0, evts, 0, EVTS_DISCOVERY.length);
        System.arraycopy(VISOR_TASK_EVTS, 0, evts, EVTS_DISCOVERY.length, VISOR_TASK_EVTS.length);

        cfg.setIncludeEventTypes(evts);

        cfg.getConnectorConfiguration().setPort(basePort);

        System.setProperty(IGNITE_JETTY_PORT, String.valueOf(basePort + 10 + gridIdx));

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        int discoPort = basePort + 20;

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:" + discoPort  + ".." + (discoPort + 10)));

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
        dataRegCfg.setName("default");
        dataRegCfg.setMetricsEnabled(true);
        dataRegCfg.setMaxSize(DFLT_DATA_REGION_INITIAL_SIZE);
        dataRegCfg.setPersistenceEnabled(true);

        DataStorageConfiguration dataStorageCfg = new DataStorageConfiguration();
        dataStorageCfg.setMetricsEnabled(true);
        
        dataStorageCfg.setDefaultDataRegionConfiguration(dataRegCfg);
        dataStorageCfg.setSystemRegionMaxSize(DFLT_DATA_REGION_INITIAL_SIZE);

        dataStorageCfg.setWalMode(LOG_ONLY);
        dataStorageCfg.setWalSegments(WAL_SEGMENTS);
        dataStorageCfg.setWalSegmentSize(WAL_SEGMENT_SZ);

        //-cfg.setDataStorageConfiguration(dataStorageCfg);

        cfg.setClientMode(client);

        return cfg;
    }
    
    static public IgniteConfiguration singleIgniteConfiguration(IgniteConfiguration cfg)
            throws IgniteCheckedException {   
    	int port = basePort.getAndAdd(50);
    	

        cfg.setGridLogger(new Slf4jLogger());
        cfg.setLocalHost("127.0.0.1");
        cfg.setEventStorageSpi(new MemoryEventStorageSpi());
        

        File workDir = new File(U.workDirectory(null, null), "launcher-work");

        cfg.setWorkDirectory(workDir.getAbsolutePath());

        int[] evts = new int[EVTS_DISCOVERY.length + VISOR_TASK_EVTS.length];

        System.arraycopy(EVTS_DISCOVERY, 0, evts, 0, EVTS_DISCOVERY.length);
        System.arraycopy(VISOR_TASK_EVTS, 0, evts, EVTS_DISCOVERY.length, VISOR_TASK_EVTS.length);

        cfg.setIncludeEventTypes(evts);

        cfg.getConnectorConfiguration().setPort(port);

        System.setProperty(IGNITE_JETTY_PORT, String.valueOf(port + 10));

        
        // Configure discovery SPI.
        IsolatedDiscoverySpi discoSpi = new IsolatedDiscoverySpi(); 
        cfg.setDiscoverySpi(discoSpi);

        
        cfg.setGridLogger(new Slf4jLogger(log));
        cfg.setMetricsLogFrequency(0);
        return cfg;
    }

    /**
     * Starts read and write from cache in background.
     *
     * @param services Distributed services on the grid.
     */
    public static void deployServices(IgniteServices services) {
    	
        services.deployNodeSingleton("serviceList", new ClusterAgentServiceList());
        services.deployNodeSingleton("loadDataService", new CacheLoadDataService());
        services.deployNodeSingleton("clearDataService", new CacheClearDataService());
        services.deployClusterSingleton("copyDataService", new CacheCopyDataService());        
        services.deployNodeSingleton("computeTaskLoadService", new ComputeTaskLoadService());
        
        //String cacheName = "default";
        //services.deployKeyAffinitySingleton("loadDataKeyAffinityService",new ClusterLoadDataService(), cacheName, "id");
    }

    /** */
    public static String registerNodeUrl(Ignite ignite) {
    	 ClusterNode node = ignite.cluster().localNode();    	 

         Collection<String> jettyAddrs = node.attribute(ATTR_REST_JETTY_ADDRS);

         if (jettyAddrs == null) {
             throw new IgniteException("Cluster: Failed to start Jetty REST server on embedded node");
         }

         String jettyHost = "127.0.0.1";
         for(String host: jettyAddrs) {
        	 if(!host.startsWith("0")) {
        		 jettyHost = host;
        	 }
         }

         Integer jettyPort = node.attribute(ATTR_REST_JETTY_PORT);

         if (F.isEmpty(jettyHost) || jettyPort == null)
             throw new IgniteException("Cluster: Failed to start Jetty REST handler on embedded node");

         log.info("Cluster: Started embedded node for data analysis purpose [TCP binary port={}, Jetty REST port={}]", ignite.configuration().getConnectorConfiguration().getPort(), jettyPort);

         String nodeUrl = String.format("http://%s:%d/%s", jettyHost, jettyPort, ignite.configuration().getIgniteInstanceName());

         ClusterHandler.registerNodeUrl(ignite.cluster().localNode().id().toString(),nodeUrl,ignite.name());
         
         return nodeUrl;
    }

    /**
     * Start ignite node with cacheEmployee and populate it with data.
     */
    public static Ignite tryStart(IgniteConfiguration cfg) {
    	Ignite ignite = null;
        if (initGuard.compareAndSet(false, true)) {
            log.info("Cluster: Starting embedded nodes for data analysis ...");

            System.setProperty(IGNITE_NO_ASCII, "true");
            System.setProperty(IGNITE_QUIET, "false");
            System.setProperty(IGNITE_UPDATE_NOTIFIER, "false");

            System.setProperty(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE, "20");
            System.setProperty(IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED, "true");

            
           
            int idx = 0;
            int port = basePort.get();

            boolean first = idx == 0;

            try {
                igniteConfiguration(cfg, port, idx, false);
                

                ignite = Ignition.start(cfg);

                if (first) {
                   
                    initLatch.countDown();
                    idx++;
                }
            }
            catch (Throwable e) {
                if (first) {
                    basePort.getAndAdd(50);

                    log.warn("Cluster: Failed to start embedded node.", e);
                }
                else
                    log.error("Cluster: Failed to start embedded node.", e);
            }
            finally {
                if (idx >0) {
                    try {
                        if (ignite != null) {
                            ignite.cluster().active(true);
                        }

                        log.info("Cluster: All embedded nodes for demo successfully started");
                    }
                    catch (Throwable ignored) {
                        log.info("Cluster: Failed to launch ignite load");
                    }

                   
                }
            }

            
        }

        return ignite;
    }

    /** */
    public static void stop(String clusterName,String nodeId) {  
    	if(nodeId!=null) {
    		try {
	    		Ignite ignite = Ignition.ignite(UUID.fromString(nodeId));
	    		String gridName = ignite.configuration().getIgniteInstanceName();
	    		Ignition.stop(gridName,true);
	    		clusterName = null;
	    		ClusterHandler.clusterUrlMap.remove(nodeId);
    		}
	    	catch(IgniteIllegalStateException e) {
	    		
	    	}
    	}
    	
    	if(clusterName!=null) {
    		Ignition.stop(clusterName,true);
    	}

        initLatch = new CountDownLatch(1);

        initGuard.compareAndSet(true, false);
    }
    
    /**
     * Start ignite node with cacheEmployee and populate it with data.
     * @throws IgniteCheckedException 
     */
    public static Ignite trySingleStart(JsonObject json) throws IgniteCheckedException {
    	Ignite ignite = null;
    	String clusterId = json.getString("id");
    	String clusterName = Utils.escapeFileName(json.getString("name"));
    	// 单个节点： clusterID和nodeID相同
    	UUID nodeID = UUID.fromString(clusterId);
    	try {
    		ignite = Ignition.ignite(UUID.fromString(clusterId));
    		return ignite;
    	}
    	catch(IgniteIllegalStateException e) {
    		
    	}
    	// 基于Instance Name 查找ignite
    	try {
    		ignite = Ignition.ignite(clusterName);
    		return ignite;
    	}
    	catch(IgniteIllegalStateException e) {
    		
    	}
    	
        if (ignite==null) {        	
        	
        	String work = U.workDirectory(null, null)+ "/config/";			
        	String cfgFile = String.format("%s%s/src/main/resources/META-INF/%s-server.xml", work, clusterName,clusterName);
			URL springCfgUrl = U.resolveSpringUrl(cfgFile);
			
			IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgMap;

			cfgMap = IgnitionEx.loadConfigurations(springCfgUrl);
			
			//only on node per jvm.					
			IgniteConfiguration cfg = cfgMap.get1().iterator().next();
			cfg.setNodeId(nodeID);
			cfg.setConsistentId(clusterId);
			cfg.setIgniteInstanceName(clusterName);
			
			AgentClusterLauncher.singleIgniteConfiguration(cfg);
			
			ignite = IgnitionEx.start(cfg,cfgMap.get2());
			
        }
        return ignite;
    }
    

    /**
     * Start ignite node with cacheEmployee and populate it with data.
     * @throws IgniteCheckedException 
     */
    public static void saveBlobToFile(JsonObject json) throws IgniteCheckedException {    	
    	String clusterId = json.getString("id");    	
    	String base64 = json.getString("blob");    	 
        String prefix = "data:application/octet-stream;base64,";
        if (base64!=null && base64.startsWith(prefix)) {        	
        	
        	String fileName = Utils.escapeFileName(json.getString("name"));
        	
        	String work = U.workDirectory(null, null)+ "/config/";
			U.mkdirs(new File(work));
			
			byte[] zip = Base64.decodeBase64(base64.substring(prefix.length()));
			File zipFile = new File(work, fileName+".zip");
			String descDir = work + fileName+"/";
			
			try {
				FileOutputStream writer = new FileOutputStream(zipFile);
				writer.write(zip);
				writer.close();
				
				AgentUtils.unZip(zipFile, descDir);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}        	
			
        }
        
    }  
   

}
