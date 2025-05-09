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

package org.apache.ignite.internal.plugin;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.BindException;
import java.net.Socket;
import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.handlers.WebSocketRouter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;



/**
 * WebSocketRouter processor provider for websocket api.
 */
public class IgniteWebSocketAgentPluginProvider implements PluginProvider<AgentConfiguration> {
	// Singerton
	private static WebSocketRouter websocketAgent;
    
    private static int counter = 0;
	 
	/** Ignite logger. */
	private IgniteLogger log;     
	
    private AgentConfiguration cfg;
    


	
    /** {@inheritDoc} */
    @Override public String name() {
        return "WebSocket Agent";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "2.16.999";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "apache copy right";
    }

    /** {@inheritDoc} */
    @Override public IgniteWebSocketPlugin plugin() {
        return new IgniteWebSocketPlugin(websocketAgent);
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
    	 IgniteConfiguration igniteCfg = ctx.igniteConfiguration();
         
         this.log = ctx.log(this.getClass());

         this.cfg = null;
         
         if (igniteCfg.getPluginConfigurations() != null) {
             for (PluginConfiguration pluginCfg : igniteCfg.getPluginConfigurations()) {
                 if (pluginCfg instanceof AgentConfiguration) {
                     cfg = (AgentConfiguration)pluginCfg;
                     break;
                 }
             }
         }      
         
         
         if(cfg!=null) {
        	 String startIniFile = U.getIgniteHome()+ "/config/default-agent.properties";
             File defaultIni = new File(startIniFile);
             if(defaultIni.exists()) {
            	 // if defaultIni exist auto enable create websocket server
            	 AgentConfiguration propCfg = new AgentConfiguration();
            	 try {
            		propCfg.load(defaultIni.toURI().toURL());
    				if(cfg!=null) {
    					cfg.merge(propCfg);
    				}
    				else {
    					cfg = propCfg;
    				}
    			} catch (IOException e) {				
    				log.error("Fail to load config/agent.properties", e);
    			}
             }         
         	counter++;
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
    	
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
    	
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() {
    	
    	 // start mongodb singerton when admin grid start
    	if(cfg!=null && websocketAgent==null) {    		      
 	       try {
 	    	   synchronized(IgniteWebSocketAgentPluginProvider.class) {
 	    		  if(websocketAgent==null) {
 	    			 websocketAgent = new WebSocketRouter(cfg);
 	    			 websocketAgent.start();
			    	 log.info("websocketServer","connect to "+cfg.serverUri());
 	    		  }
 	    	   }
 	    	   
 	       }catch(Exception e) {
 	    	   log.error("websocketServer connect fail.",e);
 	       }
     	}
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {    	
    	if(cfg!=null) {
    		counter--;
    		if(websocketAgent!=null) {
        		synchronized(IgniteWebSocketAgentPluginProvider.class) {
        			if(websocketAgent!=null && counter<=0) {
    		      	   log.info("mongoServer","shutting down "+ websocketAgent.toString());
    		      	   websocketAgent.close();
    		      	   websocketAgent = null;
        			}
        		}
          	}
    	}
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
}
