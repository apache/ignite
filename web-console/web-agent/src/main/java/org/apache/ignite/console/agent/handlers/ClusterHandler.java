package org.apache.ignite.console.agent.handlers;

import java.util.List;

import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.websocket.TopologySnapshot;

import io.vertx.core.json.JsonObject;

public interface ClusterHandler {
	
	public abstract RestResult restCommand(String clusterId,JsonObject params) throws Throwable;
    
    public abstract List<TopologySnapshot> topologySnapshot();
    
    public void close();
}
