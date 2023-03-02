package de.kp.works.ignite.query;
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


import de.kp.works.ignite.IgniteAdmin;
import de.kp.works.ignite.IgniteConstants;
import de.kp.works.ignite.IgniteResultTransform;
import de.kp.works.ignite.IgniteTransform;
import de.kp.works.ignite.ValueType;
import de.kp.works.ignite.ValueUtils;
import de.kp.works.ignite.graph.EdgeEntryIterator;
import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.graph.IgniteEdgeEntry;
import de.kp.works.ignite.graph.IgniteVertexEntry;
import de.kp.works.ignite.graph.VertexEntryIterator;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class IgniteQuery {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgniteQuery.class);

    protected IgniteCache<String, BinaryObject> cache;
    protected String sqlStatement;

    protected ElementType elementType;    
    

    public IgniteQuery(String name, IgniteAdmin admin) {

        try {
            /*
             * Retrieve element type from provided
             * cache (table) name
             */
            if (name.equals(admin.namespace() + "_" + IgniteConstants.EDGES)) {
                elementType = ElementType.EDGE;
            }
            else if (name.equals(admin.namespace() + "_" + IgniteConstants.VERTICES)) {
                elementType = ElementType.VERTEX;
            }
            else if (name.startsWith(admin.namespace() + "_")) {
                elementType = ElementType.DOCUMENT;
            }
            else if (name.startsWith(admin.namespace() + ".")) {
                elementType = ElementType.DOCUMENT;
            }
            else
                elementType = ElementType.UNDEFINED;
            /*
             * Connect to Ignite cache of the respective
             * name
             */
            cache = admin.createTable(name);

        } catch (Exception e) {
            cache = null;
        }

    }

    protected void vertexToFields(Object vertex, Direction direction, HashMap<String, String> fields) {
        /*
         * An Edge links two Vertex objects. The Direction determines
         * which Vertex is the tail Vertex (out Vertex) and which Vertex
         * is the head Vertex (in Vertex).
         *
         * [HEAD VERTEX | OUT] -- <EDGE> --> [TAIL VERTEX | IN]
         *
         * The illustration is taken from the Apache TinkerPop [Edge]
         * documentation.
         *
         * This implies: FROM = OUT & TO = IN
         */
        
        
    	if (vertex instanceof ReferenceVertexProperty) {
			ReferenceVertexProperty<?> key = (ReferenceVertexProperty) vertex;        	
        	if (direction.equals(Direction.BOTH)) {
                fields.put(IgniteConstants.TO_COL_NAME, key.id().toString());
                fields.put(IgniteConstants.FROM_COL_NAME, key.id().toString());
                fields.put(IgniteConstants.TO_TYPE_COL_NAME, key.label());
                fields.put(IgniteConstants.FROM_TYPE_COL_NAME, key.label());
            }
            if (direction.equals(Direction.IN)) {
                fields.put(IgniteConstants.TO_COL_NAME, key.id().toString());
                fields.put(IgniteConstants.TO_TYPE_COL_NAME, key.label());
            }
            else {
                fields.put(IgniteConstants.FROM_COL_NAME, key.id().toString());
                fields.put(IgniteConstants.FROM_TYPE_COL_NAME, key.label());
            }
        }
        else {
        	if (direction.equals(Direction.BOTH)) {
                fields.put(IgniteConstants.TO_COL_NAME, vertex.toString());
                fields.put(IgniteConstants.FROM_COL_NAME, vertex.toString());
            }
            if (direction.equals(Direction.IN))
                fields.put(IgniteConstants.TO_COL_NAME, vertex.toString());
            else
                fields.put(IgniteConstants.FROM_COL_NAME, vertex.toString());
        }

    }
    
    public IgniteQuery withRange(int offset, int limit) {
        if(offset>0) {
        	sqlStatement += " limit " + offset + ',' + limit;
        }
        else {
        	sqlStatement += " limit " + limit;
        }
        return this;
    }
    
    public IgniteQuery orderBy(String order, boolean desc) {
        if(desc) {
        	sqlStatement += " order by  " + order + " DESC";
        }
        else {
        	sqlStatement += " order by  " + order;
        }
        return this;
    }

    public Iterator<IgniteEdgeEntry> getEdgeEntries() {

        List<IgniteEdgeEntry> entries = new ArrayList<>();
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            return entries.iterator();

        if (!elementType.equals(ElementType.EDGE))
            return entries.iterator();

        try {
        	FieldsQueryCursor<List<?>> sqlResult = getSqlResult();
            /*
             * Parse sql result and extract edge specific entries
             */
            return parseEdges(sqlResult);

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);

        }

        return entries.iterator();
    }

    public Iterator<IgniteVertexEntry> getVertexEntries() {

        List<IgniteVertexEntry> entries = new ArrayList<>();
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            return entries.iterator();

        if (!elementType.equals(ElementType.VERTEX))
            return entries.iterator();

        try {
        	FieldsQueryCursor<List<?>> sqlResult = getSqlResult();
            /*
             * Parse sql result and extract Vertex specific entries
             */
            return parseVertices(sqlResult);

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);

        }

        return entries.iterator();
    }


    /**
     * This method returns the result of the Ignite SQL query
     * in form of a row-based result list. It supports user
     * specific read requests
     */
    public Iterator<IgniteResult> getResult() {

        List<IgniteResult> result = new ArrayList<>();
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            return result.iterator();

        try {
            if (cache == null)
                throw new Exception("Cache is not initialized.");

            
            String cacheName = cache.getName();

            if (elementType.equals(ElementType.EDGE)) {
            	FieldsQueryCursor<List<?>> sqlResult = getSqlResult();
                /*
                 * Parse sql result and extract edge specific entries
                 */
            	EdgeEntryIterator entries = parseEdges(sqlResult);
                /*
                 * Group edge entries into edge rows
                 */
                return IgniteResultTransform.transformEdgeEntries(entries);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
            	FieldsQueryCursor<List<?>> sqlResult = getSqlResult();
                /*
                 * Parse sql result and extract Vertex specific entries
                 */
            	VertexEntryIterator entries = parseVertices(sqlResult);
                /*
                 * Group vertex entries into edge rows
                 */
                return IgniteResultTransform.transformVertexEntries(entries);

            }
            else if (elementType.equals(ElementType.DOCUMENT)) {
            	 /*
                 * Parse sql result and extract Document specific entries
                 */
                List<IgniteResult> entries = getSqlResultWithMeta();                
                return entries.iterator();
            }
            else
                throw new Exception("Cache '" + cacheName +  "' is not supported.");

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);
        }
        return result.iterator();

    }
    
    private EdgeEntryIterator parseEdges(FieldsQueryCursor<List<?>> sqlResult) {
        return new EdgeEntryIterator(sqlResult);
    }

    private VertexEntryIterator parseVertices(FieldsQueryCursor<List<?>> sqlResult) {        
        return new VertexEntryIterator(sqlResult);
    }

    protected abstract void createSql(Map<String, String> fields);

    protected List<List<?>> getSqlResultList() {
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sqlStatement);
        return cache.query(sqlQuery).getAll();
    }
    
    protected FieldsQueryCursor<List<?>> getSqlResult() {
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sqlStatement);
        return cache.query(sqlQuery);
    }
    
    public List<IgniteResult> getSqlResultWithMeta() {
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sqlStatement);
        FieldsQueryCursor<List<?>> cursor = cache.query(sqlQuery);
        List<List<?>> sqlResult = cursor.getAll();
        List<IgniteResult> result = new ArrayList<>();
    	for(List<?> row: sqlResult) {
    		IgniteResult item = new IgniteResult();
    		for(int i=0;i<cursor.getColumnsCount();i++) {
    			Object value = row.get(i);
    			if(value!=null) {
    				String field = cursor.getFieldName(i);
    				if(field.equals("_KEY")) {
    					field = "id";
    				}
    				item.addColumn(field, value.getClass().getSimpleName(), value);
    			}
            }
    		result.add(item);
    	}
    	return result;
    }

    protected void buildSelectPart() throws Exception {

        if (cache == null)
            throw new Exception("Cache is not initialized.");

        List<String> columns = getColumns();

        sqlStatement = "select";

        sqlStatement += " " + String.join(",", columns);
        sqlStatement += " from " + cache.getName();

    }
    /**
     * This method retrieves the `select` columns
     * of the respective cache.
     */
    protected List<String> getColumns() throws Exception {

        List<String> columns = new ArrayList<>();
        /*
         * We want to retrieve the cache key as well
         */
        columns.add("_key");

        if (cache == null)
            throw new Exception("Cache is not initialized.");

        String cacheName = cache.getName();
        if (elementType.equals(ElementType.EDGE)) {
            /*
             * The edge identifier used by TinkerPop to
             * identify an equivalent of a data row
             */
            columns.add(IgniteConstants.ID_COL_NAME);
            /*
             * The edge identifier type to reconstruct the
             * respective value. IgniteGraph supports [Long]
             * as well as [String] as identifier.
             */
            columns.add(IgniteConstants.ID_TYPE_COL_NAME);
            /*
             * The edge label used by TinkerPop and IgniteGraph
             */
            columns.add(IgniteConstants.LABEL_COL_NAME);
            /*
             * The `TO` vertex description
             */
            columns.add(IgniteConstants.TO_COL_NAME);
            columns.add(IgniteConstants.TO_TYPE_COL_NAME);
            /*
             * The `FROM` vertex description
             */
            columns.add(IgniteConstants.FROM_COL_NAME);
            columns.add(IgniteConstants.FROM_TYPE_COL_NAME);
            /*
             * The timestamp this cache entry has been created.
             */
            columns.add(IgniteConstants.CREATED_AT_COL_NAME);
            /*
             * The timestamp this cache entry has been updated.
             */
            columns.add(IgniteConstants.UPDATED_AT_COL_NAME);
            /*
             * The property section of this cache entry
             */
            columns.add(IgniteConstants.PROPERTY_KEY_COL_NAME);
            columns.add(IgniteConstants.PROPERTY_TYPE_COL_NAME);
            /*
             * The serialized property value
             */
            columns.add(IgniteConstants.PROPERTY_VALUE_COL_NAME);
            return columns;
        }
        if (elementType.equals(ElementType.VERTEX)) {
            /*
             * The vertex identifier used by TinkerPop to identify
             * an equivalent of a data row
             */
            columns.add(IgniteConstants.ID_COL_NAME);
            /*
             * The vertex identifier type to reconstruct the
             * respective value. IgniteGraph supports [Long]
             * as well as [String] as identifier.
             */
            columns.add(IgniteConstants.ID_TYPE_COL_NAME);
            /*
             * The vertex label used by TinkerPop and IgniteGraph
             */
            columns.add(IgniteConstants.LABEL_COL_NAME);
            /*
             * The timestamp this cache entry has been created.
             */
            columns.add(IgniteConstants.CREATED_AT_COL_NAME);
            /*
             * The timestamp this cache entry has been updated.
             */
            columns.add(IgniteConstants.UPDATED_AT_COL_NAME);
            /*
             * The property section of this cache entry
             */
            columns.add(IgniteConstants.PROPERTY_KEY_COL_NAME);
            columns.add(IgniteConstants.PROPERTY_TYPE_COL_NAME);
            /*
             * The serialized property value
             */
            columns.add(IgniteConstants.PROPERTY_VALUE_COL_NAME);
            return columns;
        }
        if (elementType.equals(ElementType.DOCUMENT)) {
        	columns.add("*");
        	return columns;
        }
        throw new Exception("Cache '" + cacheName +  "' is not supported.");

    }
}
