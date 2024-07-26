package de.kp.works.janus.gremlin.plugin;

import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class GremlinPlugin implements IgnitePlugin{

	String databaseName;
	
	GraphManager graphManager;
	
	
	public GraphManager getGraphManager() {
		return graphManager;
	}
	
	public Graph getGraph() {
		return graphManager.getGraph(databaseName);
	}
	
	public GraphTraversalSource traversal() {
		return graphManager.getGraph(databaseName).traversal();
	}
	
}
