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
package de.kp.works.janus.gremlin.plugin;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.BindException;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.jetbrains.annotations.Nullable;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.server.JanusGraphServer;

import de.kp.works.ignite.IgniteContext;


/**
 * Security processor provider for tests.
 */
public class GremlinServerPluginProvider implements PluginProvider<GremlinPluginConfiguration> {

	//One Gremlin Server for all ignite instances
	public static JanusGraphServer janusGraphServer;
	
	public static Settings settings;	

	private String databaseName;

	private GremlinPlugin gremlin = new GremlinPlugin();
	
	private int counter = 0;

	/** Ignite logger. */
	private IgniteLogger log;

	private GremlinPluginConfiguration cfg;

	/** {@inheritDoc} */
	@Override
	public String name() {
		return "JanusGremlinServer";
	}

	/** {@inheritDoc} */
	@Override
	public String version() {
		return "3.7.0";
	}

	/** {@inheritDoc} */
	@Override
	public String copyright() {
		return "apache copy right";
	}

	/** {@inheritDoc} */
	@Override
	public GremlinPlugin plugin() {
		return gremlin;
	}

	/** {@inheritDoc} */
	@Override
	public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
		IgniteConfiguration igniteCfg = ctx.igniteConfiguration();

		Ignite ignite = ctx.grid();
		this.log = ctx.log(this.getClass());

		this.cfg = null;

		if (igniteCfg.getPluginConfigurations() != null) {
			for (PluginConfiguration pluginCfg : igniteCfg.getPluginConfigurations()) {
				if (pluginCfg instanceof GremlinPluginConfiguration) {
					cfg = (GremlinPluginConfiguration) pluginCfg;
					break;
				}
			}
		}
		if (cfg == null && "janus_backend".equals(ctx.grid().name()) && !ignite.configuration().isClientMode()) {
			// if node name == 'janus-backend' auto enable create gremlin server
			cfg = new GremlinPluginConfiguration();
		}

		boolean per = ignite.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration()
				.isPersistenceEnabled();
		if (cfg != null && per) {
			cfg.setPersistenceEnabled(true);
		}

		if (cfg != null) {
			try {
				databaseName = igniteCfg.getIgniteInstanceName();
				gremlin.databaseName = databaseName;
				
				// only read first settings
				if(settings==null) {
					settings = Settings.read(cfg.getGremlinServerCfg());
				}
				if(settings.graphs.isEmpty()) {
					String configBase = ctx.igniteConfiguration().getIgniteHome() + "/config/gremlin-server";
	
					File graphFile = new File(configBase, "janus-" + databaseName + ".properties");
					if (graphFile.exists()) {
						log.info(databaseName + "::load gremlim graph config file " + graphFile.getAbsolutePath());
						settings.graphs.put(databaseName, graphFile.getAbsolutePath());
					} else {
						graphFile = new File(configBase, "janus-default.properties");
						log.info(databaseName + "::load default gremlim graph config file " + graphFile.getAbsolutePath());
						settings.graphs.put(databaseName, graphFile.getAbsolutePath());
					}				
				}
				counter++;
				
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
		}

	}

	/** {@inheritDoc} */
	@SuppressWarnings("unchecked")
	@Override
	public @Nullable <T> T createComponent(PluginContext ctx, Class<T> cls) {
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void start(PluginContext ctx) {

	}

	/** {@inheritDoc} */
	@Override
	public void stop(boolean cancel) {
		
	}

	/** {@inheritDoc} */
	@Override
	public void onIgniteStart() {
		// start gremlin server singerton when janus-backend grid start
		if (janusGraphServer == null && settings != null) {
			try {
				janusGraphServer = new JanusGraphServer(cfg.getGremlinServerCfg());
		        janusGraphServer.start().exceptionally(t -> {
		        	log.error("JanusGraph Server was unable to start and will now begin shutdown", t);
		            janusGraphServer.stop().join();
		            return null;
		        }).join();
				
				gremlin.graphManager = janusGraphServer.getGremlinServer().getServerGremlinExecutor().getGraphManager();
				
				log.info("JanaGremlinServer", "listern on " + settings.host + ":" + settings.port);

			} catch (Exception e) {
				log.error("GremlinServer bind fail.", e);
				throw new RuntimeException(e);
			}
		}
	}

	

	/** {@inheritDoc} */
	@Override
	public void onIgniteStop(boolean cancel) {
		if(cfg!=null) {
			counter--;
		}
		
		if(gremlin.graphManager!=null) {			
			JanusGraph graph = (JanusGraph)gremlin.graphManager.getGraph(databaseName);
			gremlin.graphManager = null;
			if(graph!=null) {				
				graph.close();
			}
		}
		
		if (janusGraphServer!= null && counter<=0) {
			log.info("JanusGremlinServer", "shutting down " + janusGraphServer.toString());
			janusGraphServer.stop();			
			janusGraphServer = null;
		}		
	}

	/** {@inheritDoc} */
	@Override
	public @Nullable Serializable provideDiscoveryData(UUID nodeId) {
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void receiveDiscoveryData(UUID nodeId, Serializable data) {
		// No-op.
	}

	/** {@inheritDoc} */
	@Override
	public void validateNewNode(ClusterNode node) throws PluginValidationException {
		// No-op.
	}	
}
