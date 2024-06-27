package org.apache.ignite.internal.plugin;

import org.apache.ignite.console.agent.handlers.VertxClusterHandler;
import org.apache.ignite.plugin.IgnitePlugin;

import io.vertx.core.Vertx;

public class IgniteVertxPlugin implements IgnitePlugin{
  final String instanceName;
  
  public IgniteVertxPlugin(String instance) {	
	  this.instanceName = instance;
  }

  public Vertx getVertx() {	  
	  return VertxClusterHandler.clusterVertxMap.get(instanceName);
  }
}
