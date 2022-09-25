package de.kp.works.ignite.gremlin.models;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import de.kp.works.ignite.IgniteTable;
import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.gremlin.mutators.Creator;
import de.kp.works.ignite.gremlin.mutators.Mutators;
import de.kp.works.ignite.gremlin.mutators.VertexWriter;
import de.kp.works.ignite.gremlin.readers.VertexReader;

public class DocumentModel extends VertexModel {

	public DocumentModel(IgniteGraph graph, IgniteTable table) {
		super(graph, table);		
	}
	
	public VertexReader getReader() {
        return new VertexReader(graph);
    }
}
