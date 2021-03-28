package org.apache.ignite.console.agent.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;


public class ClusterLoadDataService implements ClusterAgentService {
   
	@Override
	public Map<String, ? extends Object> call(Map<String,Object> payload) {
		Map<String,Object> result = new HashMap<>();
		result.put("count", 0);
		return result;
	}

	public String toString() {
		return "load data to cluster";
	}
}
