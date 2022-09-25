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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import de.kp.works.ignite.IgniteAdmin;
import de.kp.works.ignite.IgniteConf;
import de.kp.works.ignite.IgniteConstants;
import de.kp.works.ignite.ValueUtils;
import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.gremlin.exception.IgniteGraphException;
import de.kp.works.ignite.gremlin.models.DocumentModel;
import de.kp.works.ignite.gremlin.models.EdgeModel;
import de.kp.works.ignite.gremlin.models.VertexModel;
import de.kp.works.ignite.gremlin.process.strategy.optimization.IgniteGraphStepStrategy;
import de.kp.works.ignite.gremlin.process.strategy.optimization.IgniteVertexStepStrategy;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IgniteGraph implements Graph {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteGraph.class);

    static {
        TraversalStrategies.GlobalCache.registerStrategies(IgniteGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                        IgniteVertexStepStrategy.instance(),
                        IgniteGraphStepStrategy.instance()
                ));
    }
    
    static Map<String,IgniteGraph> graphInstances = new Hashtable<>();

    private final IgniteGraphConfiguration config;

    private final IgniteGraphFeatures features;
    private final IgniteAdmin admin;

    private final EdgeModel edgeModel;
    private final VertexModel vertexModel;

    private final Cache<ByteBuffer, Edge> edgeCache;
    private final Cache<ByteBuffer, Vertex> vertexCache;
    
    public static boolean stringedPropertyType = false;

    /**
     * This method is invoked by Gremlin's GraphFactory
     * and defines the starting point for further tasks.
     */
    public static IgniteGraph open(final Configuration properties) throws IgniteGraphException {
        return new IgniteGraph(properties);
    }
    
    public static IgniteGraph getGraph(String name) throws IgniteGraphException {
        return graphInstances.get(name);
    }

    public IgniteGraph(Configuration properties) {
        this(new IgniteGraphConfiguration(properties));        
    }

    public IgniteGraph(IgniteGraphConfiguration config) throws IgniteGraphException {
        this(config, IgniteGraphUtils.getConnection(config));
    }

    public IgniteGraph(IgniteGraphConfiguration config, IgniteConnection connection) throws IgniteGraphException {

        this.config = config;
        this.admin = connection.getAdmin();

        this.features = new IgniteGraphFeatures(true);
        
        String propertyType = config.getString("gremlin.graph.propertyType");
        if(propertyType!=null && propertyType.equals("STRING")) {
        	stringedPropertyType = true;
        }
        
        String igniteCfg = config.getString("gremlin.graph.ignite.cfg");
        if(igniteCfg!=null && !igniteCfg.isEmpty()) {
        	IgniteConf.file = igniteCfg;
        }        
        
        /*
         * Create the Ignite caches that are used as the
         * backed for this graph implementation
         */
        try {
            IgniteGraphUtils.createTables(config, admin);
        } catch (Exception e) {
            throw new IgniteGraphException(e);
        }

        /* Edge & Vertex models */
        this.edgeModel = new EdgeModel(this,
                admin.getTable(IgniteGraphUtils.getTableName(config, IgniteConstants.EDGES),ElementType.EDGE));

        this.vertexModel = new VertexModel(this,
                admin.getTable(IgniteGraphUtils.getTableName(config, IgniteConstants.VERTICES),ElementType.VERTEX));

        this.edgeCache = CacheBuilder.newBuilder()
                .maximumSize(config.getElementCacheMaxSize())
                .expireAfterAccess(config.getElementCacheTtlSecs(), TimeUnit.SECONDS)
                .removalListener((RemovalListener<ByteBuffer, Edge>) notif -> ((IgniteEdge) notif.getValue()).setCached(false))
                .build();

        this.vertexCache = CacheBuilder.newBuilder()
                .maximumSize(config.getElementCacheMaxSize())
                .expireAfterAccess(config.getElementCacheTtlSecs(), TimeUnit.SECONDS)
                .removalListener((RemovalListener<ByteBuffer, Vertex>) notif -> ((IgniteVertex) notif.getValue()).setCached(false))
                .build();
        
        graphInstances.put(config.getGraphNamespace(), this);

    }
    
    public IgniteGraphConfiguration getIgniteGraphConfiguration() {
    	return config;
    	
    }
    public EdgeModel getEdgeModel() {
        return edgeModel;
    }

    public VertexModel getVertexModel() {
        return vertexModel;
    }
    
    public DocumentModel getDocumentModel(String label) {
    	if (label == null) {
            throw Exceptions.argumentCanNotBeNull("label");
        }
    	if(IgniteConstants.VERTICES.equals(label.toLowerCase()) || IgniteConstants.EDGES.equals(label.toLowerCase())) {
    		 throw Exceptions.variablesNotSupported();
    	}
    	DocumentModel vertexModel = new DocumentModel(this,admin.getTable(IgniteGraphUtils.getTableName(config, label),ElementType.EDGE));
        return vertexModel;
    }
    
    /** Document RELATED **/    
    public Vertex addDocument(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        /*
         * Vertices that define an `id` in the provided keyValues
         * use this value (long or numeric). Otherwise `null` is
         * returned.
         */
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        /*
         * The `idValue` either is a provided [Long] or a random
         * UUID as [String].
         */
        idValue = IgniteGraphUtils.generateIdIfNeeded(idValue);
        long now = System.currentTimeMillis();
        IgniteDocument newVertex = new IgniteDocument(this, idValue, label, now, now, IgniteGraphUtils.propertiesToMap(keyValues));
        newVertex.validate();
        newVertex.writeToModel();
        
        return newVertex;
    }


    /** VERTEX RELATED **/

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        /*
         * Vertices that define an `id` in the provided keyValues
         * use this value (long or numeric). Otherwise `null` is
         * returned.
         */
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        /*
         * The `idValue` either is a provided [Long] or a random
         * UUID as [String].
         */
        idValue = IgniteGraphUtils.generateIdIfNeeded(idValue);
        long now = System.currentTimeMillis();
        IgniteVertex newVertex = new IgniteVertex(this, idValue, label, now, now, IgniteGraphUtils.propertiesToMap(keyValues));
        newVertex.validate();
        newVertex.writeToModel();

        Vertex vertex = findOrCreateVertex(idValue);
        ((IgniteVertex) vertex).copyFrom(newVertex);
        return vertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        if (vertexIds.length == 0) {
            return allVertices();
        } else {
            Stream<Object> stream = Stream.of(vertexIds);
            List<Vertex> vertices = stream
                    .map(id -> {
                        if (id == null)
                            throw Exceptions.argumentCanNotBeNull("id");
                        else if (id instanceof Long)
                            return id;
                        else if (id instanceof Number)
                            return ((Number) id).longValue();
                        else if (id instanceof Vertex)
                            return ((Vertex) id).id();
                        else
                            return id;
                    })
                    .map(this::findOrCreateVertex)
                    .collect(Collectors.toList());
            getVertexModel().load(vertices);
            return vertices.stream()
                    .filter(v -> ((IgniteVertex) v).arePropertiesFullyLoaded())
                    .iterator();
        }
    }

    public Vertex vertex(Object id) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        Vertex v = findOrCreateVertex(id);
        ((IgniteVertex) v).load();
        return v;
    }

    public Vertex findOrCreateVertex(Object id) {
        return findVertex(id, true);
    }
    
  

    /**
     * Retrieve vertex from cache or build new
     * vertex instance with the provided `id`.
     */
    protected Vertex findVertex(Object id, boolean createIfNotFound) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        id = IgniteGraphUtils.generateIdIfNeeded(id);
        ByteBuffer key = ByteBuffer.wrap(ValueUtils.serialize(id));
        Vertex cachedVertex = vertexCache.getIfPresent(key);
        if (cachedVertex != null && !((IgniteVertex) cachedVertex).isDeleted()) {
            return cachedVertex;
        }
        if (!createIfNotFound) return null;
        IgniteVertex vertex = new IgniteVertex(this, id);
        vertexCache.put(key, vertex);
        vertex.setCached(true);
        return vertex;
    }
    

    public void removeVertex(Vertex vertex) {
        vertex.remove();
    }
    
    public void removeDocument(Vertex vertex) {
        vertex.remove();
    }

    /** VERTEX RETRIEVAL METHODS (see VertexModel) **/

    public Iterator<Vertex> allVertices() {
        return vertexModel.vertices();
    }

    public Iterator<Vertex> allVertices(Object fromId, int limit) {
        return vertexModel.vertices(fromId, limit);
    }

    public Iterator<Vertex> verticesByLabel(String label) {
        return vertexModel.vertices(label);
    }

    public Iterator<Vertex> verticesByLabel(String label, String key, Object value) {
        return vertexModel.vertices(label, key, value);
    }

    public Iterator<Vertex> verticesInRange(String label, String key, Object inclusiveFromValue, Object exclusiveToValue) {
        return vertexModel.verticesInRange(label, key, inclusiveFromValue, exclusiveToValue);
    }

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object fromValue, int limit) {
        return verticesWithLimit(label, key, fromValue, limit, false);
    }

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object fromValue, int limit, boolean reversed) {
        return vertexModel.verticesWithLimit(label, key, fromValue, limit, reversed);
    }

    /** EDGE RELATED **/

    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label, Object... keyValues) {
        return outVertex.addEdge(label, inVertex, keyValues);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        if (edgeIds.length == 0) {
            return allEdges();
        } else {
            Stream<Object> stream = Stream.of(edgeIds);
            List<Edge> edges = stream
                    .map(id -> {
                        if (id == null)
                            throw Exceptions.argumentCanNotBeNull("id");
                        else if (id instanceof Long)
                            return id;
                        else if (id instanceof Number)
                            return ((Number) id).longValue();
                        else if (id instanceof Edge)
                            return ((Edge) id).id();
                        else
                            return id;
                    })
                    .map(this::findOrCreateEdge)
                    .collect(Collectors.toList());
            getEdgeModel().load(edges);
            return edges.stream()
                    .filter(e -> ((IgniteEdge) e).arePropertiesFullyLoaded())
                    .iterator();
        }
    }

    public Edge edge(Object id) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        Edge edge = findOrCreateEdge(id);
        ((IgniteEdge) edge).load();
        return edge;
    }

    public Edge findOrCreateEdge(Object id) {
        return findEdge(id, true);
    }

    protected Edge findEdge(Object id, boolean createIfNotFound) {
        if (id == null) {
            throw Exceptions.argumentCanNotBeNull("id");
        }
        id = IgniteGraphUtils.generateIdIfNeeded(id);
        ByteBuffer key = ByteBuffer.wrap(ValueUtils.serialize(id));
        Edge cachedEdge = edgeCache.getIfPresent(key);
        if (cachedEdge != null && !((IgniteEdge) cachedEdge).isDeleted()) {
            return cachedEdge;
        }
        if (!createIfNotFound) {
            return null;
        }
        IgniteEdge edge = new IgniteEdge(this, id);
        edgeCache.put(key, edge);
        edge.setCached(true);
        return edge;
    }

    public void removeEdge(Edge edge) {
        edge.remove();
    }

    public Iterator<Edge> allEdges() {
        return edgeModel.edges();
    }

    public Iterator<Edge> allEdges(Object fromId, int limit) {
        return edgeModel.edges(fromId, limit);
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        throw Graph.Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }


    @Override
    public IgniteGraphConfiguration configuration() {
        return this.config;
    }

    @Override
    public Features features() {
        return features;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, IgniteGraphConfiguration.IGNITE_GRAPH_CLASS.getSimpleName().toLowerCase());
    }

    @Override
    public void close() {
        /* Do nothing */
    }

}
