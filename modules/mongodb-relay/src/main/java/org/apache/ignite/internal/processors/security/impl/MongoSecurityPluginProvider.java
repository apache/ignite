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

package org.apache.ignite.internal.processors.security.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.mongo.MongoPluginConfiguration;
import org.apache.ignite.internal.processors.mongo.MongoServerPluginProvider;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;

import static org.apache.ignite.plugin.security.SecurityPermission.*;

import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.jetbrains.annotations.Nullable;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;

/** 
 * 基于Mongodb的用户权限系统
 **/
public class MongoSecurityPluginProvider implements PluginProvider<MongoPluginConfiguration> {
	 /** Ignite logger. */
    IgniteLogger log;    
	
	MongoPluginConfiguration cfg;
	
	String databaseName;

    /** Permissions. */
    private  SecurityPermissionSet perms;

    /** Users security data. */
    private  Collection<SimpleSecurityData> clientData = new ArrayList<>();
    
    public MongoSecurityPluginProvider() {
          SecurityPermissionSet permsSet = new SecurityPermissionSetBuilder()  
  			.appendCachePermissions("INDEX.*", CACHE_READ)
            .appendTaskPermissions("*", TASK_CANCEL)
            .appendTaskPermissions("*", TASK_EXECUTE)
            .appendSystemPermissions(ADMIN_VIEW, EVENTS_ENABLE)
            .build();
          
          perms = permsSet;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "MongoSecurityProcessorProvider";
    }

    
    /** */
    public MongoSecurityPluginProvider(SecurityPermissionSet perms) {       
        this.perms = perms;
    }

	@Override
	public String version() {		
		return "1.0";
	}

	@Override
	public String copyright() {		
		return "apache copy right";
	}

	@Override
	public <T extends IgnitePlugin> T plugin() {
		return (T)new IgnitePlugin() {
            // No-op.
        };
	}

	@Override
	public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
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

	@Override
	public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
		if (ctx.igniteConfiguration().isAuthenticationEnabled() && cls.isAssignableFrom(GridSecurityProcessor.class))
            return (T)securityProcessor(((IgniteEx)ctx.grid()).context());
		return null;
	}

	
    
	@Override
	public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void start(PluginContext ctx) throws IgniteCheckedException {
		databaseName = ctx.igniteConfiguration().getIgniteInstanceName();
	}

	@Override
	public void stop(boolean cancel) throws IgniteCheckedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onIgniteStart() throws IgniteCheckedException {
		if(cfg!=null && MongoServerPluginProvider.backend!=null) {			
				
			SecurityPermissionSetBuilder permsBuilder = new SecurityPermissionSetBuilder()  
	   	  			.appendCachePermissions("INDEX.*", CACHE_READ)
	   	            .appendTaskPermissions("*", TASK_CANCEL)
	   	            .appendTaskPermissions("*", TASK_EXECUTE)
	   	            .appendSystemPermissions(ADMIN_VIEW, EVENTS_ENABLE)
	   	            ;
			Thread t = new Thread(()->{
				while(true) {
					try {
						
						Thread.sleep(60000);
						
						MongoDatabase adminDatabase = MongoServerPluginProvider.backend.adminDatabase();
						
						Document selector = new Document();
						
				   	    MongoCollection<?> collection = adminDatabase.resolveCollection("usersInfo",true);
				   	   
				   	    Iterable<Document> users = collection.handleQuery(selector, 0, 1000, null);
					   	for(Document user: users) {
					   		List<Document> privileges = MongoServerPluginProvider.backend.getUserPrivileges(user,"");
					   		for(Document privilege: privileges) {
					   			Object actions = privilege.get("actions");
					   			Document resource = privilege.getDocumnet("resource");
					   		}
					   		
					   		
					   		SimpleSecurityData data = new SimpleSecurityData((String)user.get("name"),(String)user.get("pwd"),permsBuilder.build());
					   		
					   		this.clientData.add(data);
					   		
				  		 }
					   	break;
					   	
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				
			});
			t.start();
			
		}
		
	}

	@Override
	public void onIgniteStop(boolean cancel) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public @Nullable Serializable provideDiscoveryData(UUID nodeId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void receiveDiscoveryData(UUID nodeId, Serializable data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void validateNewNode(ClusterNode node) throws PluginValidationException {
		// TODO Auto-generated method stub
		
	}
	

    /** {@inheritDoc} */
    protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
    	SimpleSecurityData root = new SimpleSecurityData("root", "123456", perms);
		
		return new SimpleSecurityProcessor(ctx, root, clientData);
		
    }

}
