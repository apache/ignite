package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;



public interface ClusterAgentService extends Service {
   
	
	public abstract ServiceResult call(String cluterId, Map<String,Object> payload);
	
	
}
