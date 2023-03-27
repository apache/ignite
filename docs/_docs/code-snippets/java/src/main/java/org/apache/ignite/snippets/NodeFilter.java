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
package org.apache.ignite.snippets;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.util.AttributeNodeFilter;
import org.junit.jupiter.api.Test;

public class NodeFilter {

	@Test
	void setNodeFilter() {

		//tag::cache-node-filter[]
		Ignite ignite = Ignition.start();

		CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>("myCache");
		
		Collection<String> consistenIdSet = new HashSet<>();
		consistenIdSet.add("consistentId1");

		//the cache will not be hosted on the provided nodes
		cacheCfg.setNodeFilter(new MyNodeFilter(consistenIdSet));

		ignite.createCache(cacheCfg);
		//end::cache-node-filter[]

		ignite.close();
	}

	@Test
	void attributeNodeFilter() {
		//tag::add-attribute[]
		IgniteConfiguration cfg = new IgniteConfiguration();
		Map<String, Object> attributes = new HashMap<String, Object>();
		attributes.put("host_myCache", true);
		cfg.setUserAttributes(attributes);

		Ignite ignite = Ignition.start(cfg);

		//end::add-attribute[]

		//tag::attribute-node-filter[]
		CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>("myCache");

		cacheCfg.setNodeFilter(new AttributeNodeFilter("host_myCache", "true"));
		//end::attribute-node-filter[]

		ignite.createCache(cacheCfg);

		ignite.close();
	}
}
