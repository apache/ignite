package de.kp.works.ignite.gremlin;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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
import de.kp.works.ignite.gremlin.models.EdgeModel;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

public class IgniteEdge extends IgniteElement implements Edge {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteEdge.class);

    private Vertex inVertex;
    private Vertex outVertex;

    public IgniteEdge(IgniteGraph graph, Object id) {
        this(graph, id, null, null, null, null, null, null);
    }

    public IgniteEdge(IgniteGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null, null, null);
    }

    public IgniteEdge(IgniteGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties,
                      Vertex inVertex, Vertex outVertex) {
        this(graph, id, label, createdAt, updatedAt, properties, properties != null, inVertex, outVertex);
    }

    public IgniteEdge(IgniteGraph graph, Object id, String label, Long createdAt, Long updatedAt, Map<String, Object> properties,
                      boolean propertiesFullyLoaded, Vertex inVertex, Vertex outVertex) {
        super(graph, id, label, createdAt, updatedAt, properties, propertiesFullyLoaded);

        this.inVertex = inVertex;
        this.outVertex = outVertex;
    }

    @Override
    public void validate() {
        /* Do nothing */
    }

    @Override
    public ElementType getElementType() {
        return ElementType.EDGE;
    }

    @Override
    public void copyFrom(IgniteElement element) {
        super.copyFrom(element);
        if (element instanceof IgniteEdge) {
            IgniteEdge copy = (IgniteEdge) element;
            if (copy.inVertex != null) this.inVertex = copy.inVertex;
            if (copy.outVertex != null) this.outVertex = copy.outVertex;
        }
    }

    @Override
    public Vertex outVertex() {
        return getVertex(Direction.OUT);
    }

    protected void setOutVertex(IgniteVertex outVertex) {
        this.outVertex = outVertex;
    }

    @Override
    public Vertex inVertex() {
        return getVertex(Direction.IN);
    }

    protected void setInVertex(IgniteVertex inVertex) {
        this.inVertex = inVertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        return direction == Direction.BOTH
                ? IteratorUtils.of(getVertex(Direction.OUT), getVertex(Direction.IN))
                : IteratorUtils.of(getVertex(direction));
    }

    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (!Direction.IN.equals(direction) && !Direction.OUT.equals(direction)) {
            throw new IllegalArgumentException("Invalid direction: " + direction);
        }

        if (inVertex == null || outVertex == null) load();

        return Direction.IN.equals(direction) ? inVertex : outVertex;
    }

    @Override
    public void remove() {
        // Get rid of the endpoints and edge themselves.
        deleteFromModel();

        setDeleted(true);
        if (!isCached()) {
            IgniteEdge cachedEdge = (IgniteEdge) graph.findEdge(id, false);
            if (cachedEdge != null) cachedEdge.setDeleted(true);
        }
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        Iterable<String> keys = getPropertyKeys();
        Iterator<String> filter = IteratorUtils.filter(keys.iterator(),
                key -> ElementHelper.keyExists(key, propertyKeys));
        return IteratorUtils.map(filter,
                key -> new IgniteProperty<>(graph, this, key, getProperty(key)));
    }

    @Override
    public <V> Property<V> property(final String key) {
        V value = getProperty(key);
        return value != null ? new IgniteProperty<>(graph, this, key, value) : Property.empty();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        if (value != null) {
            setProperty(key, value);
            return new IgniteProperty<>(graph, this, key, value);
        } else {
            removeProperty(key);
            return Property.empty();
        }
    }

    @Override
    public EdgeModel getModel() {
        return graph.getEdgeModel();
    }

    @Override
    public void writeToModel() {
        getModel().writeEdge(this);
    }

    @Override
    public void deleteFromModel() {
        getModel().deleteEdge(this);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
