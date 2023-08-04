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
package de.kp.works.ignite.gremlin.plugin;


import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.BindException;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;

import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.jetbrains.annotations.Nullable;

import de.kp.works.ignite.IgniteConnect;

/**
 * Security processor provider for tests.
 */
public class GremlinServerPluginProvider implements PluginProvider<GremlinPluginConfiguration> {
     
	 // Singerton
     public static GremlinServer gremlinServer;
     public static GraphManager backend;
     public static Settings settings;
     public static CompletableFuture<Void> serverStarted;
     
     private String databaseName;
     

     /** Ignite logger. */
	 private IgniteLogger log;     
	
     private GremlinPluginConfiguration cfg;
	
    /** {@inheritDoc} */
    @Override public String name() {
        return "GremlinServerPluginProvider";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "0.5";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "apache copy right";
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin() {
        return (T)new IgnitePlugin() {
            // No-op.
        };
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
    	 IgniteConfiguration igniteCfg = ctx.igniteConfiguration();

         Ignite ignite = ctx.grid();
         this.log = ctx.log(this.getClass());
         
         this.cfg = null;
         
         if (igniteCfg.getPluginConfigurations() != null) {
             for (PluginConfiguration pluginCfg : igniteCfg.getPluginConfigurations()) {
                 if (pluginCfg instanceof GremlinPluginConfiguration) {
                     cfg = (GremlinPluginConfiguration)pluginCfg;
                     break;
                 }
             }
         }
         if(cfg == null && "graph".equals(ctx.grid().name())) {
        	 // if node name == 'graph' auto enable create gremlin server
        	 cfg = new GremlinPluginConfiguration();
         }
         
         boolean per = ignite.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled();
         if(cfg!=null && per) {
        	 cfg.setPersistenceEnabled(true);        	 
         }
         
         if(cfg!=null) {
        	 try {
				settings = Settings.read(cfg.getGremlinServerCfg());
				// 设置默认的ignite
	 	    	IgniteConnect.defaultIgnite = ignite;
	 	    	databaseName = igniteCfg.getIgniteInstanceName();    
			} catch (Exception e) {
				log.error(e.getMessage(),e);
			}
         }       
        
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public @Nullable  <T> T createComponent(PluginContext ctx, Class<T> cls) {
        return null;
    }

    

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) {
    	String graphName = ctx.igniteConfiguration().getIgniteInstanceName();
    	if(settings!=null) {
    		
    		String configBase = ctx.igniteConfiguration().getIgniteHome()+"/config/gremlin-server";
    		
    		File graphFile = new File(configBase,"graph-"+graphName+".properties");
    		if(graphFile.exists()) {
    			log.info(graphName+ "::load gremlim graph config file "+ graphFile.getAbsolutePath());
    			settings.graphs.put(graphName, graphFile.getAbsolutePath());
    		}
    		else {
    			log.info(graphName+ "::load gremlim graph config file "+ graphFile.getAbsolutePath());
    			graphFile = new File(configBase,"graph-default.properties");
    			settings.graphs.put(graphName, graphFile.getAbsolutePath());
    		}    		
    		
    	}
    		
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
    	if(gremlinServer!=null) {
      	   log.info("gremlinServer","shutting down "+ gremlinServer.toString());
      	   gremlinServer.stop();
      	   gremlinServer = null;
      	}
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() {
    	// start mongodb singerton when admin grid start
    	if(gremlinServer==null && settings!=null) {    		      
 	       try {
 	    	   
 	    	   gremlinServer = new GremlinServer(settings, IgniteConnect.defaultIgnite.executorService());
 	    	   serverStarted = gremlinServer.start().thenAcceptAsync(GremlinServerPluginProvider::configure);
 	    	   serverStarted.join();
 	    	   printHeader();
 	    	   log.info("GremlinServer","listern on "+settings.host+":"+settings.port); 	    	   
 	    	   
 	       }catch(Exception e) {
 	    	   log.error("GremlinServer bind fail.",e);
 	    	   throw new RuntimeException(e);
 	       }
     	}
    }
    
    private static void configure(ServerGremlinExecutor serverGremlinExecutor) {
        GraphManager graphManager = serverGremlinExecutor.getGraphManager();        
        backend = graphManager;
        
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {    	
    	
    }

    /** {@inheritDoc} */
    @Override public @Nullable Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // No-op.
    }
    

    private void printHeader() {           
        log.info("TinkerPop: {}",GremlinServer.getHeader());
    }
}
