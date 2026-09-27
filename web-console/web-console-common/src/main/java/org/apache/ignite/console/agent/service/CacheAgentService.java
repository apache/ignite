package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.services.Service;


public interface CacheAgentService extends Service {   
	
	/**
	 * payload maybe null
	 * @param payload
	 * @return
	 */
	public ServiceResult call(String cacheName,Map<String,Object> payload);	
	
}
