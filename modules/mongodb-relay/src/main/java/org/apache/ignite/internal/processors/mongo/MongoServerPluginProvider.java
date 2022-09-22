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

package org.apache.ignite.internal.processors.mongo;

import java.io.IOException;
import java.io.Serializable;
import java.net.BindException;
import java.net.Socket;
import java.util.UUID;

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
import org.jetbrains.annotations.Nullable;

import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.ignite.IgniteBackend;

/**
 * Security processor provider for tests.
 */
public class MongoServerPluginProvider implements PluginProvider<MongoPluginConfiguration> {
	 private String databaseName;
	 
	 /** Ignite logger. */
	 private IgniteLogger log;
     
	
     private MongoPluginConfiguration cfg;
     
	 //Singerton
     public static MongoServer mongoServer;
     public static IgniteBackend backend;

	
    /** {@inheritDoc} */
    @Override public String name() {
        return "MongoServerPluginProvider";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "1.0";
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
                 if (pluginCfg instanceof MongoPluginConfiguration) {
                     cfg = (MongoPluginConfiguration)pluginCfg;
                     break;
                 }
             }
         }
         if(cfg == null && "admin".equals(ctx.grid().name())) {
        	 // if node name == 'admin' auto enable create mongodb server
        	 cfg = new MongoPluginConfiguration();
         }
         boolean per = ignite.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled();
         if(cfg!=null && per) {
        	 cfg.setWithBinaryStorage(true);        	 
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
    	 // start mongodb singerton when admin grid start
    	databaseName = ctx.igniteConfiguration().getIgniteInstanceName();
    	if(cfg!=null && backend ==null) {
    	   backend = new IgniteBackend(ctx.grid(),cfg);
 	       backend.setKeepBinary(cfg.isWithBinaryStorage());
    	}
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
    	if(mongoServer!=null) {
      	   log.info("mongoServer","shutting down "+ mongoServer.toString());
      	   mongoServer.shutdownNow();
      	   mongoServer = null;
      	}
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() {
    	if(cfg!=null && mongoServer==null) {    		      
 	       try {	    	  
 	    	   mongoServer = new MongoServer(backend);
 	    	   mongoServer.bind(cfg.getHost(),cfg.getPort());
 	    	   log.info("mongoServer","listern on "+cfg.getHost()+":"+cfg.getPort());
 	    	   
 	       }catch(Exception e) {
 	    	   log.error("mongoServer bind fail.",e);
 	       }
     	}
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
}
