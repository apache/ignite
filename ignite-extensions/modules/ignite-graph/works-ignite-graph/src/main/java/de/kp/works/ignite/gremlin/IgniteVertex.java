package de.kp.works.ignite.gremlin;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.gremlin.exception.IgniteGraphNotFoundException;
import de.kp.works.ignite.gremlin.models.EdgeModel;
import de.kp.works.ignite.gremlin.models.VertexModel;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;
import org.javatuples.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IgniteVertex extends IgniteElement implements Vertex {

    public IgniteVertex(IgniteGraph graph, Object id) {
        this(graph, id, null, null, null, null, false);
    }

    public IgniteVertex(IgniteGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null);
    }

    public IgniteVertex(IgniteGraph graph, Object id, String label, Long createdAt, Long updatedAt,
                        Map<String, Object> properties, boolean propertiesFullyLoaded) {
        super(graph, id, label, createdAt, updatedAt, properties, propertiesFullyLoaded);

        if (graph != null) {
            
        }
    }

    @Override
    public void validate() {
        /* Do nothing */
    }

    @Override
    public ElementType getElementType() {
        return ElementType.VERTEX;
    }

   
    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
        IgniteGraph graph = graph();
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        if(idValue==null || "".equals(idValue)) {
        	idValue = String.format("%s:>%s->%s",this.id(),label,inVertex.id());
        }
        
        long now = System.currentTimeMillis();
        IgniteEdge newEdge = new IgniteEdge(graph, idValue, label, now, now, IgniteGraphUtils.propertiesToMap(keyValues), inVertex, this);
        newEdge.validate();
        newEdge.writeToModel();

        graph.invalidateVertexReletionCache(Pair.with(this.id(),Direction.OUT));
        graph.invalidateVertexReletionCache(Pair.with(inVertex.id(),Direction.IN));
        
        Edge edge = graph.findOrCreateEdge(idValue);
        ((IgniteEdge) edge).copyFrom(newEdge);
        return edge;
    }

    @Override
    public void remove() {
        // Remove edges incident to this vertex.
        edges(Direction.BOTH).forEachRemaining(edge -> {
            try {
                edge.remove();
            } catch (IgniteGraphNotFoundException e) {
                // ignore
            }
        });

        // Get rid of the vertex.
        deleteFromModel();

        setDeleted(true);
        if (!isCached()) {
            IgniteVertex cachedVertex = (IgniteVertex) graph().findVertex(id, false);
            if (cachedVertex != null) cachedVertex.setDeleted(true);
        }
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
    	
        if (keyValues.length > 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        if (cardinality == VertexProperty.Cardinality.set) {
    		Collection<V> list = this.getProperty(key);
    		if(list==null) {
    			list = new HashSet<V>();
    		}
    		if (value != null) {
    			if(!list.contains(value)) {
    				list.add(value);
    				setProperty(key, list);
    			}
                return new IgniteVertexProperty<V>(this, key, value);
    		}
    		return VertexProperty.empty();
    	}
        else if (cardinality == VertexProperty.Cardinality.list) {
    		Collection<V> list = this.getProperty(key);
    		if(list==null) {
    			list = new ArrayList<V>();
    		}
    		if (value != null) {
    			list.add(value);
    			setProperty(key, list);
                return new IgniteVertexProperty<V>(this, key, value);
    		}
    		return VertexProperty.empty();
    	}
        else if (cardinality != VertexProperty.Cardinality.single)
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        if (value != null) {
            setProperty(key, value);
            return new IgniteVertexProperty<>(this, key, value);
        } else {
            removeProperty(key);
            return VertexProperty.empty();
        }
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        V value = getProperty(key);
        return value != null ? new IgniteVertexProperty<>(this, key, value) : VertexProperty.empty();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        Iterable<String> keys = getPropertyKeys();
        Iterator<String> filter = IteratorUtils.filter(keys.iterator(),
                key -> ElementHelper.keyExists(key, propertyKeys));
        // add@byron
        List<VertexProperty<V>> list = new ArrayList<>(propertyKeys.length);
        while(filter.hasNext()) {
        	String key = filter.next();
        	Object v = getProperty(key);
        	if(v instanceof Collection) {
        		for(Object s: (Collection)v) {
        			list.add(new IgniteVertexProperty<V>(this, key,(V)s));
        		}
        	}
        	else if(v!=null){
        		list.add(new IgniteVertexProperty<V>(this, key,(V)v));
        	}
        }
        return list.iterator();
        /**
        return IteratorUtils.map(filter,
                key -> new IgniteVertexProperty<>(this, key, getProperty(key)));
        */
    }

    /** EDGE RELATED **/

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return getEdgeModel().edges(this, direction, edgeLabels);
    }

    public Iterator<Edge> edges(final Direction direction, final String label, final String key, final Object value) {
        return getEdgeModel().edges(this, direction, label, key, value);
    }

    public Iterator<Edge> edgesInRange(final Direction direction, final String label, final String key,
                                       final Object inclusiveFromValue, final Object exclusiveToValue) {
        return getEdgeModel().edgesInRange(this, direction, label, key, inclusiveFromValue, exclusiveToValue);
    }

    public Iterator<Edge> edgesWithLimit(final Direction direction, final String label, final String key,
                                         final Object fromValue, final int limit) {
        return edgesWithLimit(direction, label, key, fromValue, limit, false);
    }

    public Iterator<Edge> edgesWithLimit(final Direction direction, final String label, final String key,
                                         final Object fromValue, final int limit, final boolean reversed) {
        return getEdgeModel().edgesWithLimit(this, direction, label, key, fromValue, limit, reversed);
    }

    /** VERTEX RELATED **/

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return getEdgeModel().vertices(this, direction, edgeLabels);
    }

    public Iterator<Vertex> vertices(final Direction direction, final String label, final String key, final Object value) {
        return getEdgeModel().vertices(this, direction, label, key, value);
    }

    public Iterator<Vertex> verticesInRange(final Direction direction, final String label, final String key,
                                            final Object inclusiveFromValue, final Object exclusiveToValue) {
        return getEdgeModel().verticesInRange(this, direction, label, key, inclusiveFromValue, exclusiveToValue);
    }

    public Iterator<Vertex> verticesWithLimit(final Direction direction, final String label, final String key,
                                              final Object fromValue, final int limit) {
        return verticesWithLimit(direction, label, key, fromValue, limit, false);
    }

    public Iterator<Vertex> verticesWithLimit(final Direction direction, final String label, final String key,
                                              final Object fromValue, final int limit, final boolean reversed) {
        return getEdgeModel().verticesWithLimit(this, direction, label, key, fromValue, limit, reversed);
    }

    @Override
    public VertexModel getModel() {
        return graph().getVertexModel();
    }
    
    public EdgeModel getEdgeModel() {
        return graph().getEdgeModel();
    }

    @Override
    public void writeToModel() {
        getModel().writeVertex(this);
    }

    @Override
    public void deleteFromModel() {
        getModel().deleteVertex(this);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
