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
import de.kp.works.ignite.mutate.*;
import de.kp.works.ignite.query.*;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.List;

public class IgniteTable extends IgniteBaseTable {

    public IgniteTable(String name, ElementType elementType, IgniteAdmin admin) {
        super(name, elementType, admin);
    }
    /**
     * This method adds or updates an Ignite cache entry;
     * note, the current implementation requires a fully
     * qualified cache entry.
     */
    public boolean put(IgnitePut ignitePut) throws Exception {

        if (admin == null) return false;
        if (!admin.igniteExists()) return false;

        try {

            if (elementType.equals(ElementType.EDGE)) {
                putEdge(ignitePut);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                putVertex(ignitePut);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported.");

            return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }
    /**
     * Delete supports deletion of an entire element
     * (edge or vertex) and also the removal of a certain
     * property.
     */
    public boolean delete(IgniteDelete igniteDelete) throws Exception {

        if (admin == null) return false;
        if (!admin.igniteExists()) return false;

        try {

            if (elementType.equals(ElementType.EDGE)) {
                deleteEdge(igniteDelete);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                deleteVertex(igniteDelete);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported.");

             return true;

        } catch (Exception e) {
            return false;
        }
    }

    public Object increment(IgniteIncrement igniteIncrement) throws Exception {

        if (admin == null) return false;
        if (!admin.igniteExists()) return false;
        /*
         * In case of an increment, the respective incremented
         * value is returned,
         */
        try {

            if (elementType.equals(ElementType.EDGE)) {
                return incrementEdge(igniteIncrement);
            }
            else if (elementType.equals(ElementType.VERTEX)) {
                return incrementVertex(igniteIncrement);
            }
            else
                throw new Exception("Table '" + name +  "' is not supported.");


        } catch (Exception e) {
            return null;
        }

    }

    public void batch(List<IgniteMutation> mutations, Object[] results) throws Exception {

         for (int i = 0; i < mutations.size(); i++) {
             /*
              * Determine the respective mutation
              */
             IgniteMutation mutation = mutations.get(i);
             if (mutation.mutationType.equals(IgniteMutationType.DELETE)) {

                 System.out.println("DELETE");
                 IgniteDelete deleteMutation = (IgniteDelete)mutation;

                 boolean success = delete(deleteMutation);
                 if (!success) {
                     /*
                      * See [Mutators]: In case of a failed delete
                      * operation an exception is returned
                      */
                     results[i] = new Exception(
                         "Deletion of element '" + deleteMutation.getId().toString() + "' failed in table '" + name + "'.");

                 }
                 else
                     results[i] = deleteMutation.getId();

             }
             else if (mutation.mutationType.equals(IgniteMutationType.INCREMENT)) {

                 IgniteIncrement incrementMutation = (IgniteIncrement)mutation;

                 Object value = increment(incrementMutation);
                 if (value == null) {
                     /*
                      * See [Mutators]: In case of a failed delete
                      * operation an exception is returned
                      */
                     results[i] = new Exception(
                             "Increment of element '" + incrementMutation.getId().toString() + "' failed in table '" + name + "'.");

                 }
                 else
                     results[i] = value;

             }
             else {

                 IgnitePut putMutation = (IgnitePut)mutation;

                 boolean success = put(putMutation);
                 if (!success) {
                     /*
                      * See [Mutators]: In case of a failed delete
                      * operation an exception is returned
                      */
                     results[i] = new Exception(
                             "Put of element '" + putMutation.getId().toString() + "' failed in table '" + name + "'.");

                 }
                 else
                     results[i] = putMutation.getId();

             }

        }
    }
    /**
     * Retrieve all elements (edges or vertices) that refer
     * to the provided list of identifiers
     */
    public IgniteResult[] get(List<Object> ids) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, admin, ids);

        List<IgniteResult> result = igniteQuery.getResult();
        return result.toArray(new IgniteResult[0]);
    }
    /**
     * Retrieve the element (edge or vertex) that refers
     * to the provided identifier
     */
    public IgniteResult get(Object id) {
        IgniteGetQuery igniteQuery = new IgniteGetQuery(name, admin, id);

        List<IgniteResult> result = igniteQuery.getResult();
        if (result.isEmpty()) return null;
        return result.get(0);
    }

    /**
     * Returns an [IgniteQuery] to retrieve all elements
     */
    public IgniteQuery getAllQuery() {
        return new IgniteAllQuery(name, admin);
    }

    /**
     * Returns an [IgniteQuery] to retrieve all elements
     * that are referenced by a certain label
     */
    public IgniteQuery getLabelQuery(String label) {
        return new IgniteLabelQuery(name, admin, label);
    }
    /**
     * Returns an [IgniteQuery] to retrieve a specified
     * number of (ordered) elements from the beginning
     * of the cache
     */
    public IgniteQuery getLimitQuery(int limit) {
        return new IgniteLimitQuery(name, admin, limit);
    }

    public IgniteQuery getLimitQuery(Object fromId, int limit) {
        return new IgniteLimitQuery(name, admin, fromId, limit);
    }

    public IgniteQuery getLimitQuery(String label, String key, Object inclusiveFrom, int limit, boolean reversed) {
        return new IgniteLimitQuery(name, admin, label, key, inclusiveFrom, limit, reversed);    }
    /**
     * Returns an [IgniteQuery] to retrieve all elements
     * that are referenced by a certain label and share
     * a certain property key and value
     */
    public IgniteQuery getPropertyQuery(String label, String key, Object value) {
        return new IgnitePropertyQuery(name, admin, label, key, value);
    }
    /**
     * Returns an [IgniteQuery] to retrieve all elements
     * that are referenced by a certain label and share
     * a certain property key and value range
     */
    public IgniteQuery getRangeQuery(String label, String key, Object inclusiveFrom, Object exclusiveTo) {
        return new IgniteRangeQuery(name, admin, label, key, inclusiveFrom, exclusiveTo);
    }

    /* EDGE READ SUPPORT */

    /**
     * Method to find all edges that refer to the provided
     * vertex that match direction and the provided labels
     */
    public IgniteQuery getEdgesQuery(Object vertexId, Direction direction, String... labels) {
        return new IgniteEdgesQuery(name, admin, vertexId, direction, labels);
    }
    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, and a property with
     * a specific value
     */
    public IgniteQuery getEdgesQuery(Object vertexId, Direction direction, String label,
                                     String key, Object value) {
        return new IgniteEdgesQuery(name, admin, vertexId, direction, label, key, value);
    }
    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, property and a range
     * of property values
     */
    public IgniteQuery getEdgesInRangeQuery(Object vertexId, Direction direction, String label,
                                       String key, Object inclusiveFromValue, Object exclusiveToValue) {
        return new IgniteEdgesInRangeQuery(name, admin, vertexId, direction, label,
                key, inclusiveFromValue, exclusiveToValue);
    }

    public IgniteQuery getEdgesWithLimitQuery(Object vertexId, Direction direction, String label,
                                      String key, Object fromValue, int limit, boolean reversed) {
         return new IgniteEdgesWithLimitQuery(name, admin, vertexId, direction, label,
                key, fromValue, limit, reversed);

    }
}
