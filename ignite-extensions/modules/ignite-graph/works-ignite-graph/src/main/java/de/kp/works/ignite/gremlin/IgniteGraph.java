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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;

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
import de.kp.works.ignite.mutate.IgnitePut;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    public transient final Cache<String, Edge> edgeCache;
    public transient final Cache<String, Vertex> vertexCache;
    public transient final Cache<Tuple, List<Edge>> vertexReletionCache;
    
    // label -> documentModel
    private Map<String,DocumentModel> documentsModel = new Hashtable<>();
    
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
        
        String propertyType = config.getString(IgniteGraphConfiguration.Keys.GRAPH_PROPERTY_TYPE);
        if(propertyType!=null && !propertyType.isEmpty()) {        	
        	ValueUtils.defaultPropertyType = ValueUtils.PropertyType.valueOf(propertyType);
        	if(ValueUtils.defaultPropertyType==ValueUtils.PropertyType.STRING) {
        	    stringedPropertyType = true;
        	}
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

        this.edgeCache = Caffeine.newBuilder()
                .maximumSize(config.getElementCacheMaxSize())
                .expireAfterAccess(config.getElementCacheTtlSecs(), TimeUnit.SECONDS)
                .removalListener((RemovalListener<String, Edge>) (k,v,cause) -> ((IgniteEdge) v).setCached(false))
                .build();

        this.vertexCache = Caffeine.newBuilder()
                .maximumSize(config.getElementCacheMaxSize())
                .expireAfterAccess(config.getElementCacheTtlSecs(), TimeUnit.SECONDS)
                .removalListener((RemovalListener<String, Vertex>) (k,v,cause) -> ((IgniteVertex) v).setCached(false))
                .build();
        
        this.vertexReletionCache = Caffeine.newBuilder()
                .maximumSize(config.getRelationshipCacheMaxSize())
                .expireAfterAccess(config.getRelationshipCacheTtlSecs(), TimeUnit.SECONDS)
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
    
    public boolean isDocumentModel(String label) {
    	String cacheName = IgniteGraphUtils.getTableName(config, label);
    	return admin.hasCache(cacheName);
    }
    
    public DocumentModel getDocumentModel(String label) {
    	if (label == null) {
            throw Exceptions.argumentCanNotBeNull("label");
        }
    	if(IgniteConstants.VERTICES.equals(label.toLowerCase()) || IgniteConstants.EDGES.equals(label.toLowerCase())) {
    		 throw Exceptions.variablesNotSupported();
    	}
    	if(documentsModel.containsKey(label)) {
    		return documentsModel.get(label);
    	}
    	DocumentModel vertexModel = new DocumentModel(this,admin.getTable(IgniteGraphUtils.getTableName(config, label),ElementType.DOCUMENT),label);
    	documentsModel.put(label, vertexModel);
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
        Object idValue = getIdValue(keyValues);
        final String label = ElementHelper.getLabelValue(keyValues).orElse("document");
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
    
    public Vertex addIndex(final IndexMetadata indexMeta) {
    	// save index define
    	Vertex index = this.addDocument(
			T.label,"index",
			T.id,indexMeta.key().toString(),
			"label",indexMeta.label(),
			"isUnique",indexMeta.isUnique(),
			"createdAt",indexMeta.createdAt(),
			"updateAt",indexMeta.updatedAt()
		);
    	// do create index
    	IgnitePut put = new IgnitePut(indexMeta.key(), ElementType.DOCUMENT);
        put.addColumn(IgniteConstants.ID_COL_NAME, "String", indexMeta.key().propertyKey());
        put.addColumn(IgniteConstants.LABEL_COL_NAME, "String", indexMeta.key().label());
        put.addColumn(IgniteConstants.PROPERTY_VALUE_COL_NAME, "Boolean", indexMeta.isUnique());
        
    	try {
			this.getDocumentModel(indexMeta.label()).getTable().createIndex(put);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return index;
    }


    /** VERTEX RELATED **/

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = null;
        // 如果_class等于document,则使用document存储
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals("_class")) {
                String _class = ((String) keyValues[i + 1]);
                if(_class.equals("document")) {
                	return this.addDocument(keyValues);
                }
            }           
        }
        
        /*
         * Vertices that define an `id` in the provided keyValues
         * use this value (long or numeric). Otherwise `null` is
         * returned.
         */
        idValue = getIdValue(keyValues);
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
        //-ByteBuffer key = ByteBuffer.wrap(ValueUtils.serialize(id));
        String key = id.toString();
        Vertex cachedVertex = vertexCache.getIfPresent(key);
        if (cachedVertex != null && !((IgniteVertex) cachedVertex).isDeleted()) {
            return cachedVertex;
        }
        if (!createIfNotFound) return null;
        
        IgniteVertex vertex = null;
        if(ValueUtils.isDocId(id)) {
        	vertex = new IgniteDocument(this, (ReferenceVertexProperty)id);
        }
        else {
        	vertex = new IgniteVertex(this, id);
        }
        
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
        return vertexModel.vertices(0,-1);
    }

    public Iterator<Vertex> allVertices(Object fromId, int limit) {
        return vertexModel.vertices(fromId, limit);
    }

    public Iterator<Vertex> verticesByLabel(String label, int offset,int limit) {
    	if(this.isDocumentModel(label)) {
    		return this.getDocumentModel(label).vertices(label,offset,limit);
    	}
        return vertexModel.vertices(label,offset,limit);
    }

    public Iterator<Vertex> verticesByLabel(String label, String key, Object value) {
    	if(this.isDocumentModel(label)) {
    		return this.getDocumentModel(label).vertices(label,key,value);
    	}
        return vertexModel.vertices(label, key, value);
    }

    public Iterator<Vertex> verticesInRange(String label, String key, Object inclusiveFromValue, Object exclusiveToValue) {
    	if(this.isDocumentModel(label)) {
    		return this.getDocumentModel(label).verticesInRange(label, key, inclusiveFromValue, exclusiveToValue);
    	}
    	return vertexModel.verticesInRange(label, key, inclusiveFromValue, exclusiveToValue);
    }
    

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object fromValue, int limit) {
    	if(this.isDocumentModel(label)) {
    		return this.getDocumentModel(label).verticesWithLimit(label, key, fromValue, limit, false);
    	}
        return verticesWithLimit(label, key, fromValue, limit, false);
    }

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object fromValue, int limit, boolean reversed) {
    	if(this.isDocumentModel(label)) {
    		return this.getDocumentModel(label).verticesWithLimit(label, key, fromValue, limit, reversed);
    	}
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
        // 已经加载了
		if(((IgniteElement)edge).arePropertiesFullyLoaded()) {
			return edge;
		}
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
        //-ByteBuffer key = ByteBuffer.wrap(ValueUtils.serialize(id));
        String key = id.toString();
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
        return edgeModel.edges(0,-1);
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
    
    public Iterator<Edge> getVertexReletionFromCache(Tuple cacheKey) {        
        List<Edge> edges = vertexReletionCache.getIfPresent(cacheKey);
        return edges != null ? IteratorUtils.filter(edges.iterator(), edge -> !((IgniteEdge) edge).isDeleted()) : null;
    }

    public void cacheVertexReletion(Tuple cacheKey, List<Edge> edges) {        
        vertexReletionCache.put(cacheKey, edges);
    }

    protected void invalidateVertexReletionCache(Tuple cacheKey) {
        if (cacheKey != null) {
        	vertexReletionCache.invalidate(cacheKey);
        }
        else {
        	vertexReletionCache.invalidateAll();
        }        
    }
    
    /**
     * Extracts the value of the {@link T#id} key from the list of arguments.
     *
     * @param keyValues a list of key/value pairs
     * @return the value associated with {@link T#id}
     */
    public static Object getIdValue(final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(T.id))
                return keyValues[i + 1];
            if (keyValues[i].equals(T.id.name()))
                return keyValues[i + 1];
        }
        return null;
    }
}
