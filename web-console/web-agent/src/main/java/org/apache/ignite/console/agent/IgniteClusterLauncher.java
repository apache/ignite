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

import static org.apache.ignite.events.EventType.EVTS_DISCOVERY;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_ADDRS;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_JETTY_PORT;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Base64;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterStartNodeResult;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.agent.handlers.RestClusterHandler;
import org.apache.ignite.console.agent.handlers.StringStreamHandler;
import org.apache.ignite.console.agent.service.CacheClearDataService;
import org.apache.ignite.console.agent.service.CacheCopyDataService;
import org.apache.ignite.console.agent.service.CacheLoadDataService;
import org.apache.ignite.console.agent.service.CacheSaveDataService;
import org.apache.ignite.console.agent.service.ClusterAgentServiceManager;
import org.apache.ignite.console.agent.service.ClusterAgentVerticleManager;
import org.apache.ignite.console.agent.service.ComputeTaskLoadService;
import org.apache.ignite.console.json.JsonBinarySerializer;
import org.apache.ignite.console.utils.BeanMerger;
import org.apache.ignite.console.utils.Utils;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.cluster.ClusterStartNodeResultImpl;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.management.IgniteCommandRegistry;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.nodestart.IgniteRemoteStartSpecification;
import org.apache.ignite.internal.util.nodestart.StartNodeCallable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.ignite.spi.discovery.isolated.IsolatedDiscoverySpi;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Launcher for cluster features like SQL and Monitoring.
 *
 * Cache will be created and populated with data to query.
 */
public class IgniteClusterLauncher implements StartNodeCallable{
    /** */
    private static final Logger log = LoggerFactory.getLogger(IgniteClusterLauncher.class);

    /** */
    private static final AtomicBoolean initGuard = new AtomicBoolean();

    private final static AtomicInteger basePort = new AtomicInteger(20700);

    /** */
    private static final int WAL_SEGMENTS = 5;

    /** WAL file segment size, 16MBytes. */
    private static final int WAL_SEGMENT_SZ = 16 * 1024 * 1024;
    
    /** Specification. */
    private final IgniteRemoteStartSpecification spec;
    
    /** Connection timeout. */
    private final int timeout;    
    
    
    /**
     * Required by Externalizable.
     */
    public IgniteClusterLauncher() {
        spec = null;
        timeout = 0;        
    }	
    
    /**
     * Constructor.
     *
     * @param spec Specification.
     * @param timeout Connection timeout.
     */
    public IgniteClusterLauncher(IgniteRemoteStartSpecification spec, int timeout) {
        assert spec != null;

        this.spec = spec;

        this.timeout = timeout;
    }
    
    static public IgniteConfiguration mergeIgniteConfiguration(IgniteConfiguration cfg,IgniteConfiguration from)
            throws IgniteCheckedException {
    	if(from!=null && cfg!=from) {
    		BeanMerger.mergeBeans(from,cfg);
    	}
        return cfg;
    }
    
    static public IgniteConfiguration singleIgniteConfiguration(IgniteConfiguration cfg,IgniteConfiguration preCfg)
            throws IgniteCheckedException {
    	
    	cfg = IgniteClusterLauncher.mergeIgniteConfiguration(cfg,preCfg);
        
        if(cfg.getLocalHost()==null) {
        	cfg.setLocalHost("127.0.0.1");
        }       
        
        if(cfg.getIncludeEventTypes()==null) {
        	int[] evts = new int[EVTS_DISCOVERY.length];
            System.arraycopy(EVTS_DISCOVERY, 0, evts, 0, EVTS_DISCOVERY.length);
        	cfg.setIncludeEventTypes(evts);
        	cfg.setEventStorageSpi(new MemoryEventStorageSpi());
        }
        
        if(cfg.getConnectorConfiguration().getPort()==ConnectorConfiguration.DFLT_TCP_PORT) {
        	int port = basePort.getAndAdd(10);
        	cfg.getConnectorConfiguration().setPort(port);
        }
        
        if(cfg.getBinaryConfiguration()==null) {
        	BinaryConfiguration binConf = new BinaryConfiguration();
        	binConf.setTypeConfigurations(new ArrayList<>());
        	cfg.setBinaryConfiguration(binConf);
        }
        
        // Custom ClusterSerializable
        BinaryTypeConfiguration jsonBinCfg = new BinaryTypeConfiguration();
        jsonBinCfg.setTypeName("io.vertx.*");
        jsonBinCfg.setSerializer(new JsonBinarySerializer());
        
        if(cfg.getBinaryConfiguration().getTypeConfigurations()==null) {
        	cfg.getBinaryConfiguration().setTypeConfigurations(new ArrayList<>());
        }
        cfg.getBinaryConfiguration().getTypeConfigurations().add(jsonBinCfg);
        
        // Configure discovery SPI.
        if(cfg.getDiscoverySpi()==null) {
	        IsolatedDiscoverySpi discoSpi = new IsolatedDiscoverySpi(); 
	        cfg.setDiscoverySpi(discoSpi);
        }
        
        cfg.setMetricsLogFrequency(0);
        return cfg;
    }

    /**
     * Starts read and write from cache in background.
     *
     * @param services Distributed services on the grid.
     */
    public static void deployServices(IgniteServices services) {    	
        
        services.deployNodeSingleton("CacheLoadDataService", new CacheLoadDataService());
        services.deployNodeSingleton("CacheSaveDataService", new CacheSaveDataService());
        services.deployNodeSingleton("CacheClearDataService", new CacheClearDataService());
        services.deployClusterSingleton("CacheCopyDataService", new CacheCopyDataService());        
        services.deployNodeSingleton("ComputeTaskLoadService", new ComputeTaskLoadService());
        
        services.deployClusterSingleton("ClusterAgentServiceManager", new ClusterAgentServiceManager());
        services.deployClusterSingleton("ClusterAgentVerticleManager", new ClusterAgentVerticleManager());
        //String cacheName = "default";
        //services.deployKeyAffinitySingleton("loadDataKeyAffinityService",new ClusterLoadDataService(), cacheName, "id");
    }

    /** */
    public static String registerNodeUrl(Ignite ignite,String clusterId) {
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

         RestClusterHandler.registerNodeUrl(clusterId,nodeUrl,ignite.name());
         
         return nodeUrl;
    }


    /** */
    public static void stopIgnite(String clusterName,String clusterId) {  
    	if(clusterId!=null) {
    		try {	    		
	    		String gridName = RestClusterHandler.clusterNameMap.get(clusterId);
	    		if(gridName!=null) {
		    		Ignition.stop(gridName,true);
		    		clusterName = null;
	    		}	    		
    		}
	    	catch(IgniteIllegalStateException | IllegalArgumentException  e) {
	    		//-log.error("Failed to stop cluster node: "+nodeId,e);
	    	}
    	}
    	
    	if(clusterName!=null) {
    		Ignition.stop(clusterName,true);
    	}
    }
    
    /**
     * Start ignite node with cacheEmployee and populate it with data.
     * @throws IgniteCheckedException 
     */
    public static Ignite trySingleStart(String clusterId,String clusterName,int nodeIndex,boolean isLastNode,String cfgFile) throws IgniteCheckedException {
    	
        return trySingleStart(clusterId,clusterName,nodeIndex,isLastNode,cfgFile,null);
    }
    
    /**
     * Start ignite node with cacheEmployee and populate it with data.
     * @throws IgniteCheckedException 
     */
    public static Ignite trySingleStart(String clusterId,String clusterName,int nodeIndex,boolean isLastNode,String cfgFile,String preCfgFile) throws IgniteCheckedException {
    	Ignite ignite = null;    	
    	
    	// 基于Instance Name 查找ignite
    	try {
    		ignite = Ignition.ignite(clusterName);
    		return ignite;
    	}
    	catch(IgniteIllegalStateException e) {
    		
    	}
    	
    	// 基于配置文件 启动 实例
    	IgniteConfiguration preCfg = null;
    	IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgMap=null;
    	if(ignite==null && preCfgFile!=null) {
			URL springPreCfgUrl = U.resolveSpringUrl(preCfgFile);			
			cfgMap = IgnitionEx.loadConfigurations(springPreCfgUrl);
			
			Collection<IgniteConfiguration> cfgList = cfgMap.get1();
			for(IgniteConfiguration cfg: cfgList) {
				if(clusterName.equals(cfg.getIgniteInstanceName()) || cfgList.size()==1){
					preCfg = cfg;
				}				
			}
		}
		
        if(ignite==null && cfgFile!=null) {
        	
			URL springCfgUrl = U.resolveSpringUrl(cfgFile);
			
			IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> cfgWorkMap;

			cfgWorkMap = IgnitionEx.loadConfigurations(springCfgUrl);			
			//only on node per jvm.					
			IgniteConfiguration cfg = cfgWorkMap.get1().iterator().next();			
			
			// 最后一个节点： clusterID和nodeID相同
			if(isLastNode) {				
				if(cfg.getConsistentId()==null)
					cfg.setConsistentId(clusterId+"_"+nodeIndex);
				
			}
			else {
				cfg.setClusterStateOnStart(ClusterState.INACTIVE);
				if(cfg.getConsistentId()==null)
					cfg.setConsistentId(clusterId+"_"+nodeIndex);		
			}
			
			if(cfg.getIgniteInstanceName()==null)
				cfg.setIgniteInstanceName(clusterName);
			
			singleIgniteConfiguration(cfg,preCfg);
			
			ignite = IgnitionEx.start(cfg,cfgWorkMap.get2());
			
        }
        
        if(cfgMap!=null && cfgMap.get1().size()>1) {
			// other ignite instance
			Collection<IgniteConfiguration> cfgList = cfgMap.get1();
			for(IgniteConfiguration cfg: cfgList) {
				if(clusterName.equals(cfg.getIgniteInstanceName())){
					if(ignite==null) {
						ignite = IgnitionEx.start(cfg,cfgMap.get2());
					}
				}
				else {
					try {
						IgnitionEx.start(cfg,cfgMap.get2());
					}
					catch(IgniteIllegalStateException | IllegalArgumentException  e) {
			    		
			    	}
				}							
			}
		}
        
        if(isLastNode && ignite!=null) {
        	try {
				Thread.sleep(1000*nodeIndex);
				while(ignite.cluster().nodes().size()<nodeIndex) {
            		Thread.sleep(1000);                    		
            	}
			} catch (InterruptedException e) {				
				e.printStackTrace();
			}
        	
			ignite.cluster().state(ClusterState.ACTIVE);			
			deployServices(ignite.services(ignite.cluster().forServers()));			
		}
        return ignite;
    }
    

    /**
     * Start ignite node with cacheEmployee and populate it with data.
     * @throws IgniteCheckedException 
     */
    public static String saveBlobToFile(JsonObject json) throws IgniteCheckedException {    	
    	String clusterName = json.getString("name");    	
    	String base64 = json.getString("blob");    	 
        String prefix = "data:application/octet-stream;base64,";
        if (base64!=null && base64.startsWith(prefix)) {        	
        	
        	String fileName = Utils.escapeFileName(clusterName);
        	
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
				
				return descDir;
				
			} catch (IOException e) {
				log.error("Failed to save zip blob data!",e);
			}
        }
        return null;
        
    } 
    
    /**
     * Start ignite node with cacheEmployee and populate it with data.
     * @throws IgniteCheckedException 
     */
    public static File saveDataToFile(String fileName,String json) throws IgniteCheckedException {    	
    	
    	String work = U.workDirectory(null, null)+ "/config/";
		U.mkdirs(new File(work));
		
		File outFile = new File(work, fileName);			
		
		try {
			FileOutputStream writer = new FileOutputStream(outFile);
			writer.write(json.getBytes());
			writer.close();				
			
		} catch (IOException e) {
			log.error("Failed to save ini data!",e);
			throw new IgniteCheckedException(e);
		}
		return outFile;	
        
    }
    
    /**
     * @param tok Token to revoke.
     */
    public static JsonObject callClusterCommand(Ignite ignite,String cmdName,JsonObject json) {
        log.info("Cluster cmd is invoking: " + cmdName);                
        
        JsonArray args = json.getJsonArray("args");        
        JsonObject stat = new JsonObject();
        
        List<String> argsList = new ArrayList<>();
        if(ignite!=null) {
	        
        	ClusterNode node = ignite.cluster().localNode();
	        Collection<String> jettyAddrs = node.attribute(ATTR_REST_JETTY_ADDRS);
	        String host = jettyAddrs.iterator().next();
	        if(host!=null && !host.isBlank()) {
		        argsList.add("--host");
		        argsList.add(host);
	        }
	        argsList.add("--port");
	        argsList.add(""+ignite.configuration().getConnectorConfiguration().getPort());
        }
        
        if(args!=null) {
        	args.forEach(e-> argsList.addAll(Arrays.asList(e.toString().split("\\s"))));
        }
        else {
        	argsList.add(cmdName);
        }
        
        StringStreamHandler outHandder = new StringStreamHandler();        
        java.util.logging.Logger logger = java.util.logging.Logger.getLogger(CommandHandler.class.getName() + "Log");
        logger.addHandler(outHandder);
        JavaLogger javaLogger = new JavaLogger(logger,!JavaLogger.isConfigured());
        CommandHandler hnd = new CommandHandler(javaLogger);
        hnd.console = null;
        boolean experimentalEnabled = true;
        if(cmdName.equals("commandList")) {
        	IgniteCommandRegistry cmdReg = null;
        	if(ignite==null) {        		
				try {
					Field registry = hnd.getClass().getDeclaredField("registry");
					registry.setAccessible(true);
	            	cmdReg = (IgniteCommandRegistry)registry.get(hnd);
	            	
				} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
					e.printStackTrace();
				}
            	
        	}
        	else {
        		cmdReg = ((IgniteEx)ignite).commandsRegistry();
        	}
        	
        	
        	List<JsonObject> results = new ArrayList<>(10);
        	Iterator<Entry<String, Command<?, ?>>> it = cmdReg.commands();
        	while (it.hasNext()) {                    
        		Entry<String, Command<?, ?>> pair = it.next();
        		Command<?, ?> c = pair.getValue();
        		try{
	        		hnd.printUsage(javaLogger,c);
	        		javaLogger.flush();
	        		String usage = outHandder.getOutput();
	        		String desc = c.description();
	        		int pos = 0;
	        		if((pos=usage.indexOf(desc))>0) {
	        			usage = usage.substring(pos+desc.length()+26).strip();
	        		}
	            	JsonObject cmd = new JsonObject();
	            	cmd.put("name", pair.getKey());
	            	cmd.put("text", desc);
	            	cmd.put("usage", usage);
	            	cmd.put("experimental", c.argClass().getSimpleName());
	            	results.add(cmd);
        		}
        		catch(Exception e) {
        			
        		}
            }           
        	
        	stat.put("result", results);
        	return stat;
        }
        int code = hnd.execute(argsList);
        stat.put("code",code);
        javaLogger.flush();
        if(code==CommandHandler.EXIT_CODE_OK) {
        	stat.put("result", hnd.getLastOperationResult());
        	stat.put("message", outHandder.getOutput());
        }
        else {
        	stat.put("message", outHandder.getOutput());
        }
        return stat;
    }
   
    @Override
	public ClusterStartNodeResult call() throws Exception {
		// start node by agent
    	// 给其他agent发送启动消息
		ClusterStartNodeResult result = new ClusterStartNodeResultImpl(spec.host(),false,"not implement!");
		return result;
	}

}
