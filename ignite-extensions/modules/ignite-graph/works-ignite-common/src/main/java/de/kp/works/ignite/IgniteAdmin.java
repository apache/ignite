package de.kp.works.ignite;


import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.graph.IgniteEdgeEntry;
import de.kp.works.ignite.graph.IgniteVertexEntry;
import de.kp.works.ignite.mutate.IgniteDelete;
import de.kp.works.ignite.mutate.IgnitePut;
import de.kp.works.ignite.query.IgniteEdgesExistQuery;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.cache.CacheException;

public class IgniteAdmin {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteAdmin.class);    
    
    private IgniteConnect connect = null;

    private final String NO_CONNECT_INITIALIZATION = "IgniteConnect is not initialized.";

    public IgniteAdmin(String namespace) {

        try {
            this.connect = IgniteConnect.getInstance(namespace);

        } catch (Exception e) {
            String message = "Connecting to Apache Ignited failed";
            LOGGER.error(message, e);
        }

    }

    public IgniteAdmin(IgniteConnect connect) {
        this.connect = connect;
    }

    public String namespace() {
        return this.connect.graphNS();
    }

    public boolean tableExists(String name) {
        return connect.cacheExists(name);
    }

    public boolean igniteExists() {

        if (this.connect == null)
            return false;

        return this.connect.getIgnite() != null;

    }

    public IgniteCache<String, BinaryObject> createTable(String name) {

        try {

            if (this.connect == null)
                throw new Exception(NO_CONNECT_INITIALIZATION);

            return this.connect.getOrCreateCache(name);

        } catch (Exception e) {
            LOGGER.error("Cache creation failed.", e);
            return null;
        }
    }

    public void dropTable(String name) throws Exception {

        if (this.connect == null)
            throw new Exception(NO_CONNECT_INITIALIZATION);

        this.connect.deleteCache(name);

    }
    /**
     * This method creates an [IgniteTable] and provides
     * access to the underlying cache. Note, this method
     * does not verify whether the cache exists or not.
     */
    public IgniteTable getTable(String tableName,ElementType elementType) {
        if (connect == null) {
            LOGGER.error(NO_CONNECT_INITIALIZATION);
            return null;
        }

        return new IgniteTable(tableName, elementType, this);
    }

    /**
     * Check whether a vertex is referenced by edges
     * either as `from` or `to` vertex
     */
    public boolean hasEdges(Object vertex, String cacheName) {
        IgniteEdgesExistQuery igniteQuery = new IgniteEdgesExistQuery(cacheName, this, vertex);
        Iterator<IgniteEdgeEntry> edges = igniteQuery.getEdgeEntries();
        return edges.hasNext();
    }

    /**
     * This method supports the deletion of an entire edge
     * or just specific properties of an existing edge.
     */
    public void deleteEdge(IgniteDelete igniteDelete, List<IgniteEdgeEntry> edge, String cacheName) {

        Ignite ignite = connect.getIgnite();
        IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName);

        List<String> cacheKeys;

        List<IgniteColumn> columns = igniteDelete.getColumns();
        /*
         * STEP #1: Check whether we must delete the
         * entire edge or just a certain column
         */
        if (columns.isEmpty()) {
            /*
             * All cache entries that refer to the specific
             * edge must be deleted.
             */
            cacheKeys = edge.stream()
                    .map(entry -> entry.cacheKey).collect(Collectors.toList());
        }
        else {
            /*
             * All cache entries that refer to a certain
             * property key must be deleted
             */
        	// modify@byron c.getColValue()->c.getColValue()
            List<String> propKeys = igniteDelete.getProperties()
                    .map(c -> c.getColName())
                    .collect(Collectors.toList());

            cacheKeys = edge.stream()
                    /*
                     * Restrict to those cache entries that refer
                     * to the provided property keys
                     */
                    .filter(entry -> propKeys.contains(entry.propKey))
                    .map(entry -> entry.cacheKey).collect(Collectors.toList());

        }

        if (!cacheKeys.isEmpty()) {
            cache.removeAll(new HashSet<>(cacheKeys));
        }
    }

    /**
     * Supports create and update operations for edges
     */
    public void writeEdge(List<IgniteEdgeEntry> entries, String cacheName) {

        Ignite ignite = connect.getIgnite();
        IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName);

        Map<String,BinaryObject> row = new TreeMap<>();
        for (IgniteEdgeEntry entry : entries) {
            BinaryObjectBuilder valueBuilder = ignite.binary().builder(cacheName);

            valueBuilder.setField(IgniteConstants.ID_COL_NAME,         entry.id);
            valueBuilder.setField(IgniteConstants.ID_TYPE_COL_NAME,    entry.idType);
            valueBuilder.setField(IgniteConstants.LABEL_COL_NAME,      entry.label);

            valueBuilder.setField(IgniteConstants.TO_COL_NAME,      entry.toId);
            valueBuilder.setField(IgniteConstants.TO_TYPE_COL_NAME, entry.toIdType);

            valueBuilder.setField(IgniteConstants.FROM_COL_NAME,      entry.fromId);
            valueBuilder.setField(IgniteConstants.FROM_TYPE_COL_NAME, entry.fromIdType);

            valueBuilder.setField(IgniteConstants.CREATED_AT_COL_NAME, entry.createdAt);
            valueBuilder.setField(IgniteConstants.UPDATED_AT_COL_NAME, entry.updatedAt);

            valueBuilder.setField(IgniteConstants.PROPERTY_KEY_COL_NAME,  entry.propKey);
            valueBuilder.setField(IgniteConstants.PROPERTY_TYPE_COL_NAME,  entry.propType);
            String value = null;
            if(entry.propType.equals("ARRAY")  || entry.propType.equals("MAP") || entry.propType.equals("SERIALIZABLE")) {
            	value = ValueUtils.serializeToString(entry.propValue);
            }
            else if(entry.propType.equals("JSON_ARRAY") || entry.propType.equals("JSON_OBJECT") || entry.propType.equals("COLLECTION")) {
            	value = ValueUtils.serializeToJsonString(entry.propValue);
            }
            else if(entry.propType.equals("BINARY")) {
            	value = Base64.getEncoder().encodeToString((byte[])entry.propValue);
            }           
            else {
            	value = entry.propValue.toString();
            }
            valueBuilder.setField(IgniteConstants.PROPERTY_VALUE_COL_NAME, value);

            String cacheKey = entry.cacheKey;
            BinaryObject cacheValue = valueBuilder.build();

            row.put(cacheKey, cacheValue);
        }
        cache.putAll(row);
        
    }

    /**
     * This method supports the deletion of an entire vertex
     * or just specific properties of an existing vertex.
     *
     * When and entire vertex must be deleted, this methods
     * also checks whether the vertex is referenced by an edge
     */
    public void deleteVertex(IgniteDelete igniteDelete, List<IgniteVertexEntry> vertex, String cacheName) throws Exception {

        Ignite ignite = connect.getIgnite();
        IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName);

        List<String> cacheKeys;

        List<IgniteColumn> columns = igniteDelete.getColumns();
        /*
         * STEP #1: Check whether we must delete the
         * entire vertex or just a certain column
         */
        if (columns.isEmpty()) {
            /*
             * All cache entries that refer to the specific
             * vertex must be deleted.
             */
            Object id = igniteDelete.getId();
            if (hasEdges(id, cacheName))
                throw new Exception("The vertex '" + id.toString() + "' is referenced by at least one edge.");

            cacheKeys = vertex.stream()
                    .map(entry -> entry.cacheKey).collect(Collectors.toList());
        }
        else {
            /*
             * All cache entries that refer to a certain
             * property key must be deleted
             */
            List<String> propKeys = igniteDelete.getProperties()
                    .map(IgniteColumn::getColName)
                    .collect(Collectors.toList());

            cacheKeys = vertex.stream()
                    /*
                     * Restrict to those cache entries that refer
                     * to the provided property keys
                     */
                    .filter(entry -> propKeys.contains(entry.propKey))
                    .map(entry -> entry.cacheKey).collect(Collectors.toList());

        }

        cache.removeAll(new HashSet<>(cacheKeys));
    }
    
    /**
     * This method supports the deletion of an entire vertex
     * or just specific properties of an existing vertex.
     *
     * When and entire vertex must be deleted, this methods
     * also checks whether the vertex is referenced by an edge
     */
    public void deleteDocument(IgniteDelete igniteDelete, String cacheName) throws Exception {

        Ignite ignite = connect.getIgnite();
        IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName);

        List<IgniteColumn> columns = igniteDelete.getColumns();
        Object rid = igniteDelete.getId();
        /*
         * STEP #1: Check whether we must delete the
         * entire vertex or just a certain column
         */
        if (columns.isEmpty()) {
            /*
             * All cache entries that refer to the specific
             * vertex must be deleted.
             */
           
            if (hasEdges(rid, cacheName))
                throw new Exception("The doc '" + rid.toString() + "' is referenced by at least one edge.");

            
            cache.remove(ValueUtils.getDocId(rid));
        }
        else {
            /*
             * All cache entries that refer to a certain
             * property key must be deleted
             */
            List<String> propKeys = igniteDelete.getProperties()
                    .map(IgniteColumn::getColName)
                    .collect(Collectors.toList());

            BinaryObject doc = cache.get(ValueUtils.getDocId(rid));
            if(doc==null) {
            	throw new Exception("The doc '" + rid.toString() + "' is not existed.");
            }
            BinaryObjectBuilder builder = doc.toBuilder();
            for(String field: propKeys) {
            	builder.removeField(field);
            }
            cache.put(ValueUtils.getDocId(rid), builder.build());

        }

        
    }

    /**
     * Supports create and update operations for vertices
     */
    public void writeVertex(List<IgniteVertexEntry> entries, String cacheName) {

        Ignite ignite = connect.getIgnite();
        IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName);

        Map<String,BinaryObject> row = new TreeMap<>();
        for (IgniteVertexEntry entry : entries) {
            BinaryObjectBuilder valueBuilder = ignite.binary().builder(cacheName);

            valueBuilder.setField(IgniteConstants.ID_COL_NAME,         entry.id);
            valueBuilder.setField(IgniteConstants.ID_TYPE_COL_NAME,    entry.idType);
            valueBuilder.setField(IgniteConstants.LABEL_COL_NAME,      entry.label);
            valueBuilder.setField(IgniteConstants.CREATED_AT_COL_NAME, entry.createdAt);
            valueBuilder.setField(IgniteConstants.UPDATED_AT_COL_NAME, entry.updatedAt);

            valueBuilder.setField(IgniteConstants.PROPERTY_KEY_COL_NAME,  entry.propKey);
            valueBuilder.setField(IgniteConstants.PROPERTY_TYPE_COL_NAME,  entry.propType);
            String value = null;
            if(entry.propType.equals("ARRAY")  || entry.propType.equals("MAP") || entry.propType.equals("SERIALIZABLE")) {
            	value = ValueUtils.serializeToString(entry.propValue);
            }
            else if(entry.propType.equals("JSON_ARRAY") || entry.propType.equals("JSON_OBJECT")) {
            	value = ValueUtils.serializeToJsonString(entry.propValue);
            }
            else if(entry.propType.equals("BINARY")) {
            	value = Base64.getEncoder().encodeToString((byte[])entry.propValue);
            }
            else if(entry.propType.equals("ENUM")) {
            	value = ValueUtils.serializeToString(entry.propValue);
            }
            else if(entry.propType.equals("COLLECTION")) { // 字符串集合
            	Collection list = ((Collection)entry.propValue);
            	int i = 0;
            	for(Object item: list) {
            		valueBuilder.setField(IgniteConstants.PROPERTY_VALUE_COL_NAME, item.toString());

                    String cacheKey = entry.cacheKey+'.'+i;
                    BinaryObject cacheValue = valueBuilder.build();

                    row.put(cacheKey, cacheValue);
                    i++;
            	}
            	continue;
            }
            else {
            	value = entry.propValue.toString();            	
            }
            valueBuilder.setField(IgniteConstants.PROPERTY_VALUE_COL_NAME, value);

            String cacheKey = entry.cacheKey;
            BinaryObject cacheValue = valueBuilder.build();

            row.put(cacheKey, cacheValue);
        }

        cache.putAll(row);
    }

    /**
     * Supports create and update operations for vertices
     */
    public void writeDocument(List<IgnitePut> entries, String cacheName) {

        Ignite ignite = connect.getIgnite();
        IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName);

        Map<String,BinaryObject> row = new TreeMap<>();
        for (IgnitePut entry : entries) {
            BinaryObjectBuilder valueBuilder = ignite.binary().builder(cacheName);
            
            for (IgniteColumn column : entry.getColumns()) {
            	if(IgniteConstants.ID_COL_NAME.equals(column.getColName())){
            		continue;
            	}
            	if(IgniteConstants.LABEL_COL_NAME.equals(column.getColName())){
            		continue;
            	}
            	valueBuilder.setField(column.getColName(), column.getColValue());
            }
            
            BinaryObject cacheValue = valueBuilder.build();           
            row.put(ValueUtils.getDocId(entry.id), cacheValue);
        }
        cache.putAll(row);
    }
    
    /**
     * Supports create and update operations for vertices
     */
    public void writeDocument(IgnitePut entry, String cacheName) {

        Ignite ignite = connect.getIgnite();
        IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName);

        
        BinaryObjectBuilder valueBuilder = ignite.binary().builder(cacheName);
        
        for (IgniteColumn column : entry.getColumns()) {
        	if(IgniteConstants.ID_COL_NAME.equals(column.getColName())){
        		continue;
        	}
        	if(IgniteConstants.LABEL_COL_NAME.equals(column.getColName())){
        		continue;
        	}
        	valueBuilder.setField(column.getColName(), column.getColValue());
        }
        
        BinaryObject cacheValue = valueBuilder.build();
       
        cache.put(ValueUtils.getDocId(entry.id), cacheValue);        
    }
    

    /**
     * Supports create and update operations for vertices index
     */
    public void createIndex(IgnitePut entry, String cacheName) {

        Ignite ignite = connect.getIgnite();
        IgniteCache<String, BinaryObject> cache = ignite.cache(cacheName);

        
        BinaryObjectBuilder valueBuilder = ignite.binary().builder(cacheName);
        
        for (IgniteColumn column : entry.getColumns()) {
        	if(IgniteConstants.ID_COL_NAME.equals(column.getColName())){
        		continue;
        	}
        	if(IgniteConstants.LABEL_COL_NAME.equals(column.getColName())){
        		continue;
        	}
        	valueBuilder.setField(column.getColName(), column.getColValue());
        }
        
        BinaryObject cacheValue = valueBuilder.build();
        
            
    }
    
    public boolean hasCache(String cacheName) {
    	Ignite ignite = connect.getIgnite();
    	if(ignite.cacheNames().contains(cacheName)) {
    		return true;
    	}
    	try {
    		ignite.cache(cacheName);
    		return true;
    	}
    	catch(CacheException e) {
    		return false;
    	}
    }
}
