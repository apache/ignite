package org.apache.ignite.console.agent.service;

import io.vertx.webmvc.mcp.McpSchema.McpToolInfo;
import io.vertx.webmvc.mcp.ToolExecutor;
import org.apache.ignite.services.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;


public interface McpService extends Service {
	
	/**
	 * defined ToolInfos
	 * @return list of McpToolInfo
	 */
	public List<ToolExecutor> toolExecutors();
	
}
