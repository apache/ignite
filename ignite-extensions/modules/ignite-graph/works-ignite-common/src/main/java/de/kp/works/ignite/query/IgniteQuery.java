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
import de.kp.works.ignite.IgniteTransform;
import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.graph.IgniteEdgeEntry;
import de.kp.works.ignite.graph.IgniteVertexEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
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
        if (direction.equals(Direction.IN))
            fields.put(IgniteConstants.TO_COL_NAME, vertex.toString());

        else
            fields.put(IgniteConstants.FROM_COL_NAME, vertex.toString());


    }

    public List<IgniteEdgeEntry> getEdgeEntries() {

        List<IgniteEdgeEntry> entries = new ArrayList<>();
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            return entries;

        if (!elementType.equals(ElementType.EDGE))
            return entries;

        try {
            List<List<?>> sqlResult = getSqlResult();
            /*
             * Parse sql result and extract edge specific entries
             */
            entries = parseEdges(sqlResult);

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);

        }

        return entries;
    }

    public List<IgniteVertexEntry> getVertexEntries() {

        List<IgniteVertexEntry> entries = new ArrayList<>();
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            return entries;

        if (!elementType.equals(ElementType.VERTEX))
            return entries;

        try {
            List<List<?>> sqlResult = getSqlResult();
            /*
             * Parse sql result and extract Vertex specific entries
             */
            entries = parseVertices(sqlResult);

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);

        }

        return entries;
    }


    /**
     * This method returns the result of the Ignite SQL query
     * in form of a row-based result list. It supports user
     * specific read requests
     */
    public List<IgniteResult> getResult() {

        List<IgniteResult> result = new ArrayList<>();
        /*
         * An empty result is returned, if the SQL statement
         * is not defined yet.
         */
        if (sqlStatement == null)
            return result;

        try {
            if (cache == null)
                throw new Exception("Cache is not initialized.");

            List<List<?>> sqlResult = getSqlResult();
            String cacheName = cache.getName();

            if (elementType.equals(ElementType.EDGE)) {
                /*
                 * Parse sql result and extract edge specific entries
                 */
                List<IgniteEdgeEntry> entries = parseEdges(sqlResult);
                /*
                 * Group edge entries into edge rows
                 */
                return IgniteTransform
                        .transformEdgeEntries(entries);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                /*
                 * Parse sql result and extract Vertex specific entries
                 */
                List<IgniteVertexEntry> entries = parseVertices(sqlResult);
                /*
                 * Group vertex entries into edge rows
                 */
                return IgniteTransform
                        .transformVertexEntries(entries);

            }
            else
                throw new Exception("Cache '" + cacheName +  "' is not supported.");

        } catch (Exception e) {
            LOGGER.error("Parsing query result failed.", e);
        }
        return result;

    }

    private List<IgniteEdgeEntry> parseEdges(List<List<?>> sqlResult) {
        /*
         * 0 : Cache key
         *
         * 1 : IgniteConstants.ID_COL_NAME (String)
         * 2 : IgniteConstants.ID_TYPE_COL_NAME (String)
         * 3 : IgniteConstants.LABEL_COL_NAME (String)
         * 4 : IgniteConstants.TO_COL_NAME (String)
         * 5 : IgniteConstants.TO_TYPE_COL_NAME (String)
         * 6 : IgniteConstants.FROM_COL_NAME (String)
         * 7 : IgniteConstants.FROM_TYPE_COL_NAME (String)
         * 8 : IgniteConstants.CREATED_AT_COL_NAME (Long)
         * 9 : IgniteConstants.UPDATED_AT_COL_NAME (Long)
         * 10: IgniteConstants.PROPERTY_KEY_COL_NAME (String)
         * 11: IgniteConstants.PROPERTY_TYPE_COL_NAME (String)
         * 12: IgniteConstants.PROPERTY_VALUE_COL_NAME (String)
         */
        return sqlResult.stream().map(result -> {
            String cacheKey = (String)result.get(0);

            String id     = (String)result.get(1);
            String idType = (String)result.get(2);
            String label  = (String)result.get(3);

            String toId     = (String)result.get(4);
            String toIdType = (String)result.get(5);

            String fromId     = (String)result.get(6);
            String fromIdType = (String)result.get(7);

            Long createdAt  = (Long)result.get(8);
            Long updatedAt  = (Long)result.get(9);

            String propKey   = (String)result.get(10);
            String propType  = (String)result.get(11);
            String propValue = (String)result.get(12);

            return new IgniteEdgeEntry(
                    cacheKey,
                    id,
                    idType,
                    label,
                    toId,
                    toIdType,
                    fromId,
                    fromIdType,
                    createdAt,
                    updatedAt,
                    propKey,
                    propType,
                    propValue);

        }).collect(Collectors.toList());
    }

    private List<IgniteVertexEntry> parseVertices(List<List<?>> sqlResult) {
        /*
         * 0 : Cache key
         *
         * 1 : IgniteConstants.ID_COL_NAME (String)
         * 2 : IgniteConstants.ID_TYPE_COL_NAME (String)
         * 3 : IgniteConstants.LABEL_COL_NAME (String)
         * 4 : IgniteConstants.CREATED_AT_COL_NAME (Long)
         * 5 : IgniteConstants.UPDATED_AT_COL_NAME (Long)
         * 6 : IgniteConstants.PROPERTY_KEY_COL_NAME (String)
         * 7 : IgniteConstants.PROPERTY_TYPE_COL_NAME (String)
         * 8 : IgniteConstants.PROPERTY_VALUE_COL_NAME (String)
         */
        return sqlResult.stream().map(result -> {

            String cacheKey = (String)result.get(0);

            String id     = (String)result.get(1);
            String idType = (String)result.get(2);

            String label  = (String)result.get(3);

            Long createdAt  = (Long)result.get(4);
            Long updatedAt  = (Long)result.get(5);

            String propKey   = (String)result.get(6);
            String propType  = (String)result.get(7);
            String propValue = (String)result.get(8);

            return new IgniteVertexEntry(
                    cacheKey,
                    id,
                    idType,
                    label,
                    createdAt,
                    updatedAt,
                    propKey,
                    propType,
                    propValue);

        }).collect(Collectors.toList());
    }

    protected abstract void createSql(Map<String, String> fields);

    protected List<List<?>> getSqlResult() {
        SqlFieldsQuery sqlQuery = new SqlFieldsQuery(sqlStatement);
        return cache.query(sqlQuery).getAll();
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
        throw new Exception("Cache '" + cacheName +  "' is not supported.");

    }
}
