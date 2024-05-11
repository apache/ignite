package de.kp.works.ignite.gremlin.models;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import de.kp.works.ignite.IgniteResultTransform;
import de.kp.works.ignite.IgniteTable;
import de.kp.works.ignite.gremlin.IgniteGraph;

import de.kp.works.ignite.gremlin.readers.DocumentReader;
import de.kp.works.ignite.query.IgniteResult;

public class DocumentModel extends VertexModel {
	final String label;

	public DocumentModel(IgniteGraph graph, IgniteTable table, String label) {		
		super(graph, table);
		this.label = label;
	}
	
	@Override
	public DocumentReader getReader() {		
        return new DocumentReader(graph, label);
    }
	

    /**
     * This method retrieves all vertices that refer to
     * the same label.
     */
    public Iterator<Vertex> vertices(String sql, Object[] args) {
    	List<IgniteResult> it = this.getTable().getIgniteSqlQuery(sql,args).getSqlResultWithMeta();
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
    	DocumentReader parser = getReader();       

        return IgniteResultTransform
                .map(it.iterator(),parser::parse);
    }

	
}
