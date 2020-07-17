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
