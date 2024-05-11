package de.kp.works.ignite.gremlin.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.graph.IgniteEdgeEntry;
import de.kp.works.ignite.gremlin.CloseableIteratorUtils;
import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.gremlin.IndexMetadata;
import de.kp.works.ignite.gremlin.models.DocumentModel;
import de.kp.works.ignite.query.IgniteResult;

public class SqlToGremlinCompiler {
	
	private final Traversal.Admin<?, ?> traversal;
	private final IgniteGraph graph;
	
	public SqlToGremlinCompiler(Traversal.Admin<?, ?> traversal) {
		this.traversal = traversal;
		this.graph = (IgniteGraph)traversal.getGraph().get();
	}

	public Traversal<Vertex, ?> select(GraphTraversalSource g, String table, String sql, Object[] args){
			
		DocumentModel model = this.graph.getDocumentModel(table);
		Iterator<Vertex> vs = model.vertices(sql,args);
		ArrayList<Object> list = new ArrayList<>();
		while(vs.hasNext()) {
			list.add(vs.next().id());
		}		
		if(list.size()>0) {
			GraphTraversal<Vertex, Vertex> graphTraversal = g.V(list.toArray());
			return graphTraversal;
		}
		else {
			return new DefaultGraphTraversal<>(g);
		}
	}
	
	
	public Traversal<Vertex, ?> execute(GraphTraversalSource g, String label, Object[] args){		
		
		Vertex v = graph.addDocument(T.label,label,args);
		GraphTraversal<Vertex, Vertex> graphTraversal = g.V(v.id());
		return graphTraversal;
	}
	
	public Traversal<Vertex, ?> addIndex(GraphTraversalSource g,  String label,final String propertyKey,Boolean isUnique){		
		Long createTime = System.currentTimeMillis();
		IndexMetadata indexMeta = new IndexMetadata(
				ElementType.DOCUMENT, label, propertyKey,
				isUnique==null? false: isUnique, IndexMetadata.State.BUILDING, createTime,createTime);
		Vertex v = graph.addIndex(indexMeta);
		GraphTraversal<Vertex, Vertex> graphTraversal = g.V(v.id());
		return graphTraversal;
	}
}
