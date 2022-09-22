package de.kp.works.ignite;
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
import de.kp.works.ignite.graph.IgniteEdgeEntry;
import de.kp.works.ignite.graph.IgniteVertexEntry;
import de.kp.works.ignite.mutate.IgniteDelete;
import de.kp.works.ignite.mutate.IgniteIncrement;
import de.kp.works.ignite.mutate.IgnitePut;
import de.kp.works.ignite.query.IgniteEdgeQuery;
import de.kp.works.ignite.query.IgniteGetQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class IgniteBaseTable {

    protected final String name;
    protected final IgniteAdmin admin;

    protected final ElementType elementType;

    public IgniteBaseTable(String name, IgniteAdmin admin) {

        this.name = name;
        this.admin = admin;

        if (name.equals(admin.namespace() + "_" + IgniteConstants.EDGES)) {
            elementType = ElementType.EDGE;
        }
        else if (name.equals(admin.namespace() + "_" + IgniteConstants.VERTICES)) {
            elementType = ElementType.VERTEX;
        }
        else
            elementType = ElementType.UNDEFINED;

        admin.createTable(name);

    }

    /** METHODS TO SUPPORT BASIC CRUD OPERATIONS **/

    protected Object incrementEdge(IgniteIncrement igniteIncrement) {
       Object edgeId = igniteIncrement.getId();
        List<IgniteEdgeEntry> edge = getEdge(edgeId);
        /*
         * We expect that the respective edge exists;
         * returning `null` leads to an exception that
         * is moved to the user interface
         */
        if (edge.isEmpty()) return null;
        return incrementEdge(igniteIncrement, edge);
    }

    protected Object incrementVertex(IgniteIncrement igniteIncrement) {
        Object vertexId = igniteIncrement.getId();
        List<IgniteVertexEntry> vertex = getVertex(vertexId);
        /*
         * We expect that the respective vertex exists;
         * returning `null` leads to an exception that
         * is moved to the user interface
         */
        if (vertex.isEmpty()) return null;
        return incrementVertex(igniteIncrement, vertex);
    }

    protected void putEdge(IgnitePut ignitePut) throws Exception {
        /*
         * STEP #1: Retrieve existing edge entries
         * that refer to the provided id
         */
        Object edgeId = ignitePut.getId();
        List<IgniteEdgeEntry> edge = getEdge(edgeId);

        if (edge.isEmpty())
            createEdge(ignitePut);

        else
            updateEdge(ignitePut, edge);

    }

    protected void putVertex(IgnitePut ignitePut) throws Exception {
        Object vertexId = ignitePut.getId();
        List<IgniteVertexEntry> vertex = getVertex(vertexId);

        if (vertex.isEmpty())
            createVertex(ignitePut);

        else
            updateVertex(ignitePut, vertex);
    }

    /**
     * The current version of [IgniteGraph] supports two different
     * approaches to retrieve an instance of an [Edge]:
     *
     * Either by identifier or by `from` and `to` fields
     */
    protected void deleteEdge(IgniteDelete igniteDelete) throws Exception {

        List<IgniteEdgeEntry> edge;
        Object edgeId = igniteDelete.getId();
        if (edgeId != null) {
            /*
             * This is the default approach to retrieve the
             * instance of an [Edge]
             */
            edge = getEdge(edgeId);
        }
        else {
            /*
             * Retrieve `from` and `to` columns
             */
            IgniteColumn fromColumn = igniteDelete.getColumn(IgniteConstants.FROM_COL_NAME);
            IgniteColumn toColumn = igniteDelete.getColumn(IgniteConstants.TO_COL_NAME);

            if (fromColumn == null || toColumn == null)
                throw new Exception("At least one of the columns `from` and `or` are missing.");

            Object fromId = fromColumn.getColValue();
            Object toId = toColumn.getColValue();

            edge = getEdge(fromId, toId);

        }

        if (!edge.isEmpty()) deleteEdge(igniteDelete, edge);
    }

    protected void deleteVertex(IgniteDelete igniteDelete) throws Exception {
        Object vertexId = igniteDelete.getId();
        List<IgniteVertexEntry> vertex = getVertex(vertexId);

        if (!vertex.isEmpty()) deleteVertex(igniteDelete, vertex);
    }
    /**
     * Create, update & delete operation for edges
     * are manipulating operations of the respective
     * cache entries.
     *
     * Reminder: A certain edge is defined as a list
     * of (edge) cache entries
     */
    protected List<IgniteEdgeEntry> getEdge(Object id) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, admin, id);
        return igniteQuery.getEdgeEntries();
    }

    protected List<IgniteEdgeEntry> getEdge(Object fromId, Object toId) {
        IgniteEdgeQuery igniteQuery = new IgniteEdgeQuery(name, admin, fromId, toId);
        return igniteQuery.getEdgeEntries();
    }
    /**
     * Create, update & delete operation for vertices
     * are manipulating operations of the respective
     * cache entries.
     *
     * Reminder: A certain vertex is defined as a list
     * of (vertex) cache entries
     */
    protected List<IgniteVertexEntry> getVertex(Object id) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, admin, id);
        return igniteQuery.getVertexEntries();
    }
    /**
     * The provided [IgnitePut] is transformed into a list of
     * [IgniteEdgeEntry] and these entries are put into cache
     */
    protected void createEdge(IgnitePut ignitePut) throws Exception {

        String id         = null;
        String idType     = null;
        String label      = null;
        String toId       = null;
        String toIdType   = null;
        String fromId     = null;
        String fromIdType = null;

        long createdAt = System.currentTimeMillis();
        long updatedAt = System.currentTimeMillis();

        List<IgniteEdgeEntry> entries = new ArrayList<>();
        /*
         * STEP #1: Move through all columns and
         * determine the common fields of all entries
         */
        for (IgniteColumn column : ignitePut.getColumns()) {
            switch (column.getColName()) {
                case IgniteConstants.ID_COL_NAME: {
                    id = column.getColValue().toString();
                    idType = column.getColType();
                    break;
                }
                case IgniteConstants.LABEL_COL_NAME: {
                    label = column.getColValue().toString();
                    break;
                }
                case IgniteConstants.TO_COL_NAME: {
                    toId = column.getColValue().toString();
                    toIdType = column.getColType();
                    break;
                }
                case IgniteConstants.FROM_COL_NAME: {
                    fromId = column.getColValue().toString();
                    fromIdType = column.getColType();
                    break;
                }
                case IgniteConstants.CREATED_AT_COL_NAME: {
                    createdAt = Long.parseLong((String) column.getColValue());
                    break;
                }
                case IgniteConstants.UPDATED_AT_COL_NAME: {
                    updatedAt = Long.parseLong((String) column.getColValue());
                    break;
                }
                default:
                    break;
            }
        }
        /*
         * Check whether the core fields of an edge entry
         * are provided
         */
        if (id == null || idType == null || label == null || toId == null || toIdType == null || fromId == null || fromIdType == null)
            throw new Exception("Number of parameters provided is not sufficient to create an edge.");

        /*
         * STEP #2: Move through all property columns
         */
        for (IgniteColumn column : ignitePut.getColumns()) {
            switch (column.getColName()) {
                case IgniteConstants.ID_COL_NAME:
                case IgniteConstants.LABEL_COL_NAME:
                case IgniteConstants.TO_COL_NAME:
                case IgniteConstants.FROM_COL_NAME:
                case IgniteConstants.CREATED_AT_COL_NAME:
                case IgniteConstants.UPDATED_AT_COL_NAME: {
                    break;
                }
                default: {
                    /*
                     * Build an entry for each property
                     */
                    String propKey   = column.getColName();
                    String propType  = column.getColType();
                    String propValue = column.getColValue().toString();
                    /*
                     * For a create request, we must generate
                     * a unique cache key for each entry
                     */
                    String cacheKey = UUID.randomUUID().toString();

                    entries.add(new IgniteEdgeEntry(
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
                            propValue));

                    break;
                }
            }
        }
        /*
         * STEP #3: Check whether the entries are still empty,
         * i.e. a vertex without properties will be created
         */
        if (entries.isEmpty()) {
            String emptyValue = "*";
            /*
             * For a create request, we must generate
             * a unique cache key for each entry
             */
            String cacheKey = UUID.randomUUID().toString();
            entries.add(new IgniteEdgeEntry(cacheKey,
                    id, idType, label, toId, toIdType, fromId, fromIdType,
                    createdAt, updatedAt, emptyValue, emptyValue, emptyValue));

        }
        /*
         * STEP #4: Persist all entries that describe the edge
         * to the Ignite edge cache
         */
        writeEdge(entries);
    }

    /**
     * This method supports the modification of an existing
     * edge; this implies the update of existing property
     * values as well as the creation of new properties
     *
     * TODO ::
     * The current implementation does not support any
     * transactions to ensure consistency.
     */
    protected void updateEdge(IgnitePut ignitePut, List<IgniteEdgeEntry> edge) {
        /*
         * STEP #1: Retrieve all properties that are
         * provided
         */
        List<IgniteColumn> properties = ignitePut.getProperties()
                .collect(Collectors.toList());
        /*
         * STEP #2: Distinguish between those properties
         * that are edge properties already and determine
         * those that do not exist
         */
        List<IgniteColumn> knownProps = new ArrayList<>();
        List<IgniteColumn> unknownProps = new ArrayList<>();

        for (IgniteColumn property : properties) {
            String propKey = property.getColName();
            if (edge.stream().anyMatch(entry -> entry.propKey.equals(propKey)))
                knownProps.add(property);

            else
                unknownProps.add(property);
        }
        /*
         * STEP #3: Update known properties
         */
        List<IgniteEdgeEntry> updatedEntries = edge.stream()
                .map(entry -> {
                    String propKey = entry.propKey;
                    /*
                     * Determine provided values that matches
                     * the property key of the entry
                     */
                    IgniteColumn property = knownProps.stream()
                            .filter(p -> p.getColName().equals(propKey)).collect(Collectors.toList()).get(0);

                    Object newValue = property.getColValue();
                    return new IgniteEdgeEntry(
                            entry.cacheKey,
                            entry.id,
                            entry.idType,
                            entry.label,
                            entry.toId,
                            entry.toIdType,
                            entry.fromId,
                            entry.fromIdType,
                            entry.createdAt,
                            /*
                             * Update the entry's `updatedAt` timestamp
                             */
                            System.currentTimeMillis(),
                            entry.propKey,
                            entry.propType,
                            newValue.toString());

                })
                .collect(Collectors.toList());

        writeEdge(updatedEntries);
        /*
         * STEP #4: Add unknown properties; note, we use the first
         * edge entry as a template for the common parameters
         */
        IgniteEdgeEntry template = edge.get(0);
        List<IgniteEdgeEntry> newEntries = unknownProps.stream()
                .map(property -> {
                    /*
                     * For a create request, we must generate
                     * a unique cache key for each entry
                     */
                    String cacheKey = UUID.randomUUID().toString();
                    return new IgniteEdgeEntry(
                            cacheKey,
                            template.id,
                            template.idType,
                            template.label,
                            template.toId,
                            template.toIdType,
                            template.fromId,
                            template.fromIdType,
                            System.currentTimeMillis(),
                            System.currentTimeMillis(),
                            property.getColName(),
                            property.getColType(),
                            property.getColValue().toString());
                })
                .collect(Collectors.toList());

        writeEdge(newEntries);
    }
    /**
     * This method supports the deletion of an entire edge
     * or just specific properties of an existing edge.
     */
    protected void deleteEdge(IgniteDelete igniteDelete, List<IgniteEdgeEntry> edge) {
        admin.deleteEdge(igniteDelete, edge, name);
    }
    /**
     * This method increments a certain property value
     */
    protected Object incrementEdge(IgniteIncrement igniteIncrement, List<IgniteEdgeEntry> edge) {
        IgniteColumn column = igniteIncrement.getColumn();
        if (column == null) return null;
        /*
         * Check whether the column value is a [Long]
         */
        String colType = column.getColType();
        if (!colType.equals(ValueType.LONG.name()))
            return null;
        /*
         * Restrict to that edge entry that refer to the
         * provided column
         */
        String colName = column.getColName();
        String colValue = column.getColValue().toString();

        List<IgniteEdgeEntry> entries = edge.stream()
                .filter(entry -> entry.propKey.equals(colName) && entry.propType.equals(colType) && entry.propValue.equals(colValue))
                .collect(Collectors.toList());

        if (entries.isEmpty()) return null;
        IgniteEdgeEntry entry = entries.get(0);

        long oldValue = Long.parseLong(entry.propValue);
        Long newValue = oldValue + 1;

        IgniteEdgeEntry newEntry = new IgniteEdgeEntry(
                entry.cacheKey,
                entry.id,
                entry.idType,
                entry.label,
                entry.toId,
                entry.toIdType,
                entry.fromId,
                entry.fromIdType,
                entry.createdAt,
                System.currentTimeMillis(),
                entry.propKey,
                entry.propType,
                newValue.toString());

        writeEdge(Collections.singletonList(newEntry));
        return newValue;
    }
    /**
     * The provided [IgnitePut] is transformed into a list of
     * [IgniteVertexEntry] and these entries are put into cache
     */
    protected void createVertex(IgnitePut ignitePut) throws Exception {

        String id = null;
        String idType = null;
        String label = null;
        long createdAt = System.currentTimeMillis();
        long updatedAt = System.currentTimeMillis();

        List<IgniteVertexEntry> entries = new ArrayList<>();
        /*
         * STEP #1: Move through all columns and
         * determine the common fields of all entries
         */
        for (IgniteColumn column : ignitePut.getColumns()) {
            switch (column.getColName()) {
                case IgniteConstants.ID_COL_NAME: {
                    id = column.getColValue().toString();
                    idType = column.getColType();
                    break;
                }
                case IgniteConstants.LABEL_COL_NAME: {
                    label = column.getColValue().toString();
                    break;
                }
                case IgniteConstants.CREATED_AT_COL_NAME: {
                    createdAt = Long.parseLong((String) column.getColValue());
                    break;
                }
                case IgniteConstants.UPDATED_AT_COL_NAME: {
                    updatedAt = Long.parseLong((String) column.getColValue());
                    break;
                }
                default:
                    break;
            }
        }
        /*
         * Check whether the core fields of a vertex entry
         * are provided
         */
        if (id == null || idType == null || label == null)
            throw new Exception("Number of parameters provided is not sufficient to create a vertex.");
        /*
         * STEP #2: Move through all property columns
         */
        for (IgniteColumn column : ignitePut.getColumns()) {
            switch (column.getColName()) {
                case IgniteConstants.ID_COL_NAME:
                case IgniteConstants.LABEL_COL_NAME:
                case IgniteConstants.CREATED_AT_COL_NAME:
                case IgniteConstants.UPDATED_AT_COL_NAME: {
                    break;
                }
                default: {
                    /*
                     * Build an entry for each property
                     */
                    String propKey   = column.getColName();
                    String propType  = column.getColType();
                    String propValue = column.getColValue().toString();
                    /*
                     * For a create request, we must generate
                     * a unique cache key for each entry
                     */
                    String cacheKey = UUID.randomUUID().toString();

                    entries.add(new IgniteVertexEntry(
                            cacheKey,
                            id,
                            idType,
                            label,
                            createdAt,
                            updatedAt,
                            propKey,
                            propType,
                            propValue));

                    break;
                }
            }
        }
        /*
         * STEP #3: Check whether the entries are still empty,
         * i.e. a vertex without properties will be created
         */
        if (entries.isEmpty()) {
            String emptyValue = "*";
            /*
             * For a create request, we must generate
             * a unique cache key for each entry
             */
            String cacheKey = UUID.randomUUID().toString();
            entries.add(new IgniteVertexEntry(cacheKey,
                    id, idType, label, createdAt, updatedAt, emptyValue, emptyValue, emptyValue));

        }
        /*
         * STEP #4: Persist all entries that describe the vertex
         * to the Ignite vertex cache
         */
        writeVertex(entries);

    }
    /**
     * This method supports the modification of an existing
     * vertex; this implies the update of existing property
     * values as well as the creation of new properties
     */
    protected void updateVertex(IgnitePut ignitePut, List<IgniteVertexEntry> vertex) {
        /*
         * STEP #1: Retrieve all properties that are
         * provided
         */
        List<IgniteColumn> properties = ignitePut.getProperties()
                .collect(Collectors.toList());
        /*
         * STEP #2: Distinguish between those properties
         * that are edge properties already and determine
         * those that do not exist
         */
        List<IgniteColumn> knownProps = new ArrayList<>();
        List<IgniteColumn> unknownProps = new ArrayList<>();

        for (IgniteColumn property : properties) {
            String propKey = property.getColName();
            if (vertex.stream().anyMatch(entry -> entry.propKey.equals(propKey)))
                knownProps.add(property);

            else
                unknownProps.add(property);
        }
        /*
         * STEP #3: Update known properties
         */
        List<IgniteVertexEntry> updatedEntries = vertex.stream()
                .map(entry -> {
                    String propKey = entry.propKey;
                    /*
                     * Determine provided values that matches
                     * the property key of the entry
                     */
                    IgniteColumn property = knownProps.stream()
                            .filter(p -> p.getColName().equals(propKey)).collect(Collectors.toList()).get(0);

                    Object newValue = property.getColValue();
                    return new IgniteVertexEntry(
                            entry.cacheKey,
                            entry.id,
                            entry.idType,
                            entry.label,
                            entry.createdAt,
                            /*
                             * Update the entry's `updatedAt` timestamp
                             */
                            System.currentTimeMillis(),
                            entry.propKey,
                            entry.propType,
                            newValue.toString());

                })
                .collect(Collectors.toList());

        writeVertex(updatedEntries);
        /*
         * STEP #4: Add unknown properties; note, we use the first
         * vertex entry as a template for the common parameters
         */
        IgniteVertexEntry template = vertex.get(0);
        List<IgniteVertexEntry> newEntries = unknownProps.stream()
                .map(property -> {
                    /*
                     * For a create request, we must generate
                     * a unique cache key for each entry
                     */
                    String cacheKey = UUID.randomUUID().toString();
                    return new IgniteVertexEntry(
                            cacheKey,
                            template.id,
                            template.idType,
                            template.label,
                            System.currentTimeMillis(),
                            System.currentTimeMillis(),
                            property.getColName(),
                            property.getColType(),
                            property.getColValue().toString());
                })
                .collect(Collectors.toList());

        writeVertex(newEntries);
    }
    /**
     * This method supports the deletion of an entire vertex
     * or just specific properties of an existing vertex.
     *
     * When and entire vertex must be deleted, this methods
     * also checks whether the vertex is referenced by an edge
     */
    protected void deleteVertex(IgniteDelete igniteDelete, List<IgniteVertexEntry> vertex) throws Exception {
        admin.deleteVertex(igniteDelete, vertex, name);
    }

    protected Object incrementVertex(IgniteIncrement igniteIncrement, List<IgniteVertexEntry> vertex) {
        IgniteColumn column = igniteIncrement.getColumn();
        if (column == null) return null;
        /*
         * Check whether the column value is a [Long]
         */
        String colType = column.getColType();
        if (!colType.equals(ValueType.LONG.name()))
            return null;
        /*
         * Restrict to that vertex entry that refer to the
         * provided column
         */
        String colName = column.getColName();
        String colValue = column.getColValue().toString();

        List<IgniteVertexEntry> entries = vertex.stream()
                .filter(entry -> entry.propKey.equals(colName) && entry.propType.equals(colType) && entry.propValue.equals(colValue))
                .collect(Collectors.toList());

        if (entries.isEmpty()) return null;
        IgniteVertexEntry entry = entries.get(0);

        long oldValue = Long.parseLong(entry.propValue);
        Long newValue = oldValue + 1;

        IgniteVertexEntry newEntry = new IgniteVertexEntry(
                entry.cacheKey,
                entry.id,
                entry.idType,
                entry.label,
                entry.createdAt,
                System.currentTimeMillis(),
                entry.propKey,
                entry.propType,
                newValue.toString());

        writeVertex(Collections.singletonList(newEntry));
        return newValue;
    }

    /**
     * Supports create and update operations for edges
     */
    private void writeEdge(List<IgniteEdgeEntry> entries) {
        admin.writeEdge(entries, name);
    }
    /**
     * Supports create and update operations for vertices
     */
    private void writeVertex(List<IgniteVertexEntry> entries) {
        admin.writeVertex(entries, name);
   }

}
