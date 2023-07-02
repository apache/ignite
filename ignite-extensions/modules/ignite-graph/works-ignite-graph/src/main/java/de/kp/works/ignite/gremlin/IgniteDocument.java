package de.kp.works.ignite.gremlin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.gremlin.models.DocumentModel;

public class IgniteDocument extends IgniteVertex {

	/**
	 * 
	 * @param graph
	 * @param id [stringId,label]
	 */
	public IgniteDocument(IgniteGraph graph, ReferenceVertexProperty id) {
		super(graph, id, id.label(), null, null, null, false);		
	}
	
	public IgniteDocument(IgniteGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null);
    }

    public IgniteDocument(IgniteGraph graph, Object id, String label, Long createdAt, Long updatedAt,
                        Map<String, Object> properties, boolean propertiesFullyLoaded) {
        super(graph, id instanceof ReferenceVertexProperty? id: new ReferenceVertexProperty<>(id,label,id), label, createdAt, updatedAt, properties, propertiesFullyLoaded);       
    }

	@Override
    public ElementType getElementType() {
        return ElementType.DOCUMENT;
    }
	
	 @Override
    public DocumentModel getModel() {		
        return graph().getDocumentModel(this.label);
    }
	 
	 @Override
    public void writeToModel() {
        getModel().writeVertex(this);
    }
	
}
