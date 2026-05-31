package org.apache.ignite.internal.plugin;

import org.apache.ignite.console.agent.handlers.WebSocketRouter;
import org.apache.ignite.plugin.IgnitePlugin;


public class IgniteWebSocketPlugin implements IgnitePlugin{
  final WebSocketRouter router;
  
  public IgniteWebSocketPlugin(WebSocketRouter router) {	
	  this.router = router;
  }

  public WebSocketRouter getWebSocketRouter() {	  
	  return router;
  }
}
