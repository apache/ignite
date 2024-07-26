package de.kp.works.ignite.gremlin.plugin;

import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.structure.Graph;

import de.kp.works.ignite.gremlin.sql.IgniteGraphTraversalSource;

public class GremlinPlugin implements IgnitePlugin{

	String databaseName;
	
	String graphConfigFile;
	
	GraphManager graphManager;	
	
	
	public GraphManager getGraphManager() {
		return graphManager;
	}
	
	public Graph getGraph() {
		return graphManager.getGraph(databaseName);
	}
	
	public IgniteGraphTraversalSource traversal() {
		return graphManager.getGraph(databaseName).traversal(IgniteGraphTraversalSource.class);
	}
	
}
