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

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.ignite.IgniteClusterManager;

/**
 * WebSocketRouter processor provider for websocket api.
 */
public class IgniteVertxPluginProvider implements PluginProvider<PluginConfiguration> {

	private static int counter = 0;

	private IgniteVertxPlugin plugin;

	private VertxOptions options = new VertxOptions();

	/** Ignite logger. */
	private IgniteLogger log;

	private String instanceName;

	/** {@inheritDoc} */
	@Override
	public String name() {
		return "Vertx";
	}

	/** {@inheritDoc} */
	@Override
	public String version() {
		return "4.5.4";
	}

	/** {@inheritDoc} */
	@Override
	public String copyright() {
		return "apache copy right";
	}

	/** {@inheritDoc} */
	@Override
	public IgniteVertxPlugin plugin() {
		// start Vertx singerton when grid start		
		return this.plugin;
	}

	/** {@inheritDoc} */
	@Override
	public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
		IgniteConfiguration igniteCfg = ctx.igniteConfiguration();

		this.log = ctx.log(this.getClass());
		this.instanceName = igniteCfg.getIgniteInstanceName();
		IgniteClusterManager clusterManager = new IgniteClusterManager(ctx.grid());
		
		options.setClusterManager(clusterManager);
		options.setHAEnabled(false);
		options.setHAGroup(this.instanceName);
		options.setEventLoopPoolSize(igniteCfg.getSystemThreadPoolSize());
		
		this.plugin = new IgniteVertxPlugin(options,log);
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
		
	}

	/** {@inheritDoc} */
	@Override
	public void onIgniteStop(boolean cancel) {
		Vertx vertx = this.plugin.getVertx();
		if (vertx != null) {
			counter--;
			synchronized (IgniteVertxPluginProvider.class) {
				if (vertx != null) {
					log.info("[Vertx web]", "shutting down " + vertx.toString());
					vertx.close();
					this.plugin.vertx = null;
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
