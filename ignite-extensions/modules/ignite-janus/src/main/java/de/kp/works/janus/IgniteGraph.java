package de.kp.works.janus;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import de.kp.works.ignite.IgniteClient;
import de.kp.works.ignite.IgniteContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphFactory.Builder;
import org.janusgraph.core.schema.JanusGraphManagement;

public class IgniteGraph {

	private static final String STORAGE_BACKEND = "storage.backend";
	private static final String STORAGE_BACKEND_IMPL = "de.kp.works.janus.IgniteStoreManager";

	private static final String GREMLIN_GRAPH = "gremlin.graph";
	private static final String GREMLIN_GRAPH_IMPL = "org.janusgraph.core.JanusGraphFactory";
	/*
	 * The list of predefined Ignite caches that are leveraged by JanusGraph
	 */
	private static final List<String> cacheNames = new ArrayList<>();
	/*
	 * The static reference to the Apache Ignite client; [IgniteContext] is designed
	 * as a singleton and this call is used to initialize this client
	 */
	private static IgniteContext igniteContext;

	private static IgniteGraph instance;
	/*
	 * Reference to the JanusGraph configuration
	 */
	private static Properties janusConfig;
	private static JanusGraph janusGraph;

	private IgniteGraph(IgniteConfiguration igniteConfig, Properties janusConfig) throws Exception {
		/*
		 * Prepare Apache Ignite caches that are leveraged by JanusGraph; note, this
		 * must be done before any graph operations is initiated (even the configuration
		 * of a certain graph)
		 */
		igniteContext = IgniteContext.getInstance(igniteConfig);

		Ignite ignite = igniteContext.getIgnite();
		if (ignite == null)
			throw new Exception("[IgniteGraph] A problem occurred while trying to initialize Apache Ignite.");

		buildIgniteBackend(ignite);
		buildJanusGraph();

	}

	/**
	 * Retrieve an instance of IgniteGraph without any externally provided
	 * configuration options
	 */
	public static IgniteGraph getInstance() throws Exception {
		return getInstance(null, null);
	}

	public static IgniteGraph getInstance(IgniteConfiguration config, Properties properties) throws Exception {

		janusConfig = properties;
		if (janusConfig == null) {

			janusConfig = new Properties();
			/*
			 * Minimal configuration to make sure that the Apache Ignite backend is used
			 */
			janusConfig.setProperty(STORAGE_BACKEND, STORAGE_BACKEND_IMPL);
			janusConfig.setProperty(GREMLIN_GRAPH, GREMLIN_GRAPH_IMPL);

			janusConfig.setProperty("storage.hostname", "localhost");

		}

		if (instance == null)
			instance = new IgniteGraph(config, janusConfig);
		return instance;

	}

	public Ignite getIgnite() {
		return igniteContext.getIgnite();
	}

	public static JanusGraph getJanus() {
		return janusGraph;
	}

	public static void closeJanus() {
		if (janusGraph == null)
			return;
		janusGraph.close();
	}

	public static void buildJanusSchema(Map<String, Class<?>> schema) throws Exception {

		if (janusGraph == null)
			throw new Exception("[IgniteGraph] JanusGraph is not initialized.");

		if (schema == null) return;

		JanusGraphManagement management = janusGraph.openManagement();

		for (Entry<String, Class<?>> field: schema.entrySet()) {

			String propertyName = field.getKey();
			Class<?> propertyType = field.getValue();

			/* Define a certain property */
			if (!management.containsPropertyKey(propertyName)) {
				management.makePropertyKey(propertyName).dataType(propertyType).make();
			}
			
		}

		management.commit();

	}

	private static void buildJanusGraph() {

		Builder graphBuilder = JanusGraphFactory.build();

		for (Map.Entry<Object, Object> entry : janusConfig.entrySet()) {

			String key = (String) entry.getKey();
			String val = (String) entry.getValue();

			graphBuilder.set(key, val);

		}

		janusGraph = graphBuilder.open();

	}

	private static void buildIgniteBackend(Ignite ignite) {
		/*
		 * This cache holds the Janus graph properties and is associated by the cache
		 * that holds read and write locks
		 */
		cacheNames.add("system_properties");
		cacheNames.add("system_properties_lock_");
		/*
		 * This cache is used to hold graph specific data that make global retrieval
		 * operations more efficient on large graphs.
		 */
		cacheNames.add("graphindex");
		cacheNames.add("graphindex_lock_");
		/*
		 * This cache is a common cache for all identities generated by the JanusGraph
		 * identity manager
		 */
		cacheNames.add("janusgraph_ids");
		/*
		 * This cache manages all logs on system level
		 */
		cacheNames.add("systemlog");
		/*
		 * This cache manages all logs on transaction level
		 */
		cacheNames.add("txlog");
		/*
		 * This store covers vertex properties as as well edge related information and
		 * is associated by a cache that holds read and write locks
		 */
		cacheNames.add("edgestore");
		cacheNames.add("edgestore_lock_");

		IgniteClient client = new IgniteClient(ignite);
		for (String cacheName : cacheNames) {
			/*
			 * Rebalancing is called here in case of partitioned Apache Ignite caches; the
			 * default configuration, however, is to use replicated caches
			 */
			client.getOrCreateCache(cacheName).rebalance().get();
		}

	}
}
