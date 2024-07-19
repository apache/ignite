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
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import org.jetbrains.annotations.Nullable;
import de.kp.works.ignite.IgniteConnect;


/**
 * GremlinServer processor provider for ignite.
 */
public class GremlinServerPluginProvider implements PluginProvider<GremlinPluginConfiguration> {

	//  One server for all ignite Instance
	public static GremlinServer gremlinServer;
	
	public static Settings settings;
	
	private static int counter = 0;

	private String databaseName;

	private GremlinPlugin gremlin = new GremlinPlugin();

	/** Ignite logger. */
	private IgniteLogger log;

	private GremlinPluginConfiguration cfg;

	/** {@inheritDoc} */
	@Override
	public String name() {
		return "GremlinServer";
	}

	/** {@inheritDoc} */
	@Override
	public String version() {
		return "3.7.2";
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
		if (cfg == null && "graph".equals(ctx.grid().name()) && !ignite.configuration().isClientMode()) {
			// if node name == 'graph' auto enable create gremlin server
			cfg = new GremlinPluginConfiguration();
		}

		boolean per = igniteCfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled();
		if (cfg != null && per) {
			cfg.setPersistenceEnabled(true);
		}

		if (cfg != null) {
			try {
				databaseName = igniteCfg.getIgniteInstanceName();
				gremlin.databaseName = databaseName;
				if(settings==null) {
					settings = Settings.read(cfg.getGremlinServerCfg());
				}
				
				String configBase = ctx.igniteConfiguration().getIgniteHome() + "/config/gremlin-server";

				File graphFile = new File(configBase, "ignite-" + databaseName + ".properties");
				if (graphFile.exists()) {
					log.info(databaseName + "::load gremlim graph config file " + graphFile.getAbsolutePath());
					gremlin.graphConfigFile =  graphFile.getAbsolutePath();
				} else {
					graphFile = new File(configBase, "ignite-default.properties");
					log.info(databaseName + "::load default gremlim graph config file " + graphFile.getAbsolutePath());
					gremlin.graphConfigFile = graphFile.getAbsolutePath();
				}	
				
				settings.graphs.put(databaseName, gremlin.graphConfigFile);
				
				// 设置默认的ignite
				IgniteConnect.defaultIgnite = ignite;
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
		// start gremlin server singerton when admin grid start
		if (gremlinServer == null && settings != null) {
			try {
				ExecutorService workerPool = ((IgniteEx) IgniteConnect.defaultIgnite).context().pools().getRestExecutorService();
				if (workerPool == null) {
					workerPool = ((IgniteEx) IgniteConnect.defaultIgnite).context().pools().getServiceExecutorService();
				}
				gremlinServer = new GremlinServer(settings, workerPool);				

				CompletableFuture<Void> serverStarted = gremlinServer.start().thenAcceptAsync(this::configure);
				serverStarted.join();

				log.info("TinkerPop: {}\n", GremlinServer.getHeader());
				log.info("GremlinServer", "listern on " + settings.host + ":" + settings.port);

			} catch (Exception e) {
				log.error("GremlinServer bind fail.", e);
				throw new RuntimeException(e);
			}
		}		
		
	}

	private void configure(ServerGremlinExecutor serverGremlinExecutor) {
		gremlin.graphManager = serverGremlinExecutor.getGraphManager();		

	}

	/** {@inheritDoc} */
	@Override
	public void onIgniteStop(boolean cancel) {
		
		if(gremlin.graphManager!=null) {			
			try {
				gremlin.graphManager.removeGraph(gremlin.databaseName);
			} 
			catch (Exception e) {				
				e.printStackTrace();
			}
			gremlin.graphManager = null;			
		}
		
		if(cfg!=null) {
			counter--;
			if (counter<=0 && gremlinServer!= null) {
				try {
					log.info("GremlinServer", "shutting down " + gremlinServer.toString());
					gremlinServer.stop();
					gremlinServer = null;
				} catch (Exception e) {
					log.error("GremlinServer close fail.", e);
				}
			}
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
