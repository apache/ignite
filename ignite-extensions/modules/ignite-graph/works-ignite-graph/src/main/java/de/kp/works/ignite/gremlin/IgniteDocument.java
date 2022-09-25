package de.kp.works.ignite.gremlin;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.gremlin.models.DocumentModel;
import de.kp.works.ignite.gremlin.models.VertexModel;

public class IgniteDocument extends IgniteVertex {

	public IgniteDocument(IgniteGraph graph, Object id) {
		super(graph, id);		
	}
	
	public IgniteDocument(IgniteGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null);
    }

    public IgniteDocument(IgniteGraph graph, Object id, String label, Long createdAt, Long updatedAt,
                        Map<String, Object> properties, boolean propertiesFullyLoaded) {
        super(graph, id, label, createdAt, updatedAt, properties, propertiesFullyLoaded);       
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
