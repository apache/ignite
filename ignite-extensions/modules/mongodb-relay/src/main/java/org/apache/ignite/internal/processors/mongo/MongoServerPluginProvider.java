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
	// Singerton
    public static MongoServer mongoServer;
    public static IgniteBackend backend;
    
	private String databaseName;
	 
	/** Ignite logger. */
	private IgniteLogger log;
     
	
    private MongoPluginConfiguration cfg;
    
    private MongoPlugin mongoPlugin = new MongoPlugin();
    
    private int counter = 0;

	
    /** {@inheritDoc} */
    @Override public String name() {
        return "MongoServer";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "3.6.4";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "apache copy right";
    }

    /** {@inheritDoc} */
    @Override public MongoPlugin plugin() {
        return mongoPlugin;
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
         if(cfg == null && "admin".equals(ctx.grid().name()) && !ignite.configuration().isClientMode()) {
        	 // if node name == 'admin' auto enable create mongodb server
        	 cfg = new MongoPluginConfiguration();
         }
         boolean per = ignite.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled();
         if(cfg!=null && per) {
        	 cfg.setWithBinaryStorage(true);        	 
         }
         
         if(cfg!=null) {
        	
         	databaseName = igniteCfg.getIgniteInstanceName();
         	if(backend==null) {
	         	backend = new IgniteBackend(ctx.grid(),cfg);
	      	    backend.setKeepBinary(cfg.isWithBinaryStorage());
         	}
         	this.mongoPlugin.backend = backend;
         	this.mongoPlugin.databaseName =	databaseName;
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
    	if(cfg!=null && mongoServer==null) {    		      
 	       try {
 	    	   synchronized(MongoServerPluginProvider.class) {
 	    		  if(mongoServer==null) {
			    	   mongoServer = new MongoServer(backend);
			    	   mongoServer.bind(cfg.getHost(),cfg.getPort());
			    	   log.info("mongoServer","listern on "+cfg.getHost()+":"+cfg.getPort());
 	    		  }
 	    	   }
 	    	   
 	       }catch(Exception e) {
 	    	   log.error("mongoServer bind fail.",e);
 	       }
     	}
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {    	
    	if(cfg!=null) {
    		counter--;
    		if(mongoServer!=null) {
        		synchronized(MongoServerPluginProvider.class) {
        			if(mongoServer!=null && counter<=0) {
    		      	   log.info("mongoServer","shutting down "+ mongoServer.toString());
    		      	   mongoServer.shutdownNow();
    		      	   mongoServer = null;
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
