package de.kp.works.ignite.gremlin.models;
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

import com.google.common.collect.Streams;

import de.kp.works.ignite.query.IgniteQuery;
import de.kp.works.ignite.IgniteResultTransform;
import de.kp.works.ignite.IgniteTable;
import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.gremlin.IgniteVertex;
import de.kp.works.ignite.gremlin.mutators.*;
import de.kp.works.ignite.gremlin.readers.EdgeReader;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class EdgeModel extends ElementModel {

    public EdgeModel(IgniteGraph graph, IgniteTable table) {
        super(graph, table);
    }

    public EdgeReader getReader() {
        return new EdgeReader(graph);
    }

    /** WRITE & DELETE **/

    public void writeEdge(Edge edge) {
        Creator creator = new EdgeWriter(graph, edge);
        Mutators.create(table, creator);
    }

    public void deleteEdge(Edge edge) {
        Mutator writer = new EdgeRemover(graph, edge);
        Mutators.write(table, writer);
    }

    /** READ **/

    public Iterator<Edge> edges(int offset,int limit) {
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        final EdgeReader parser = new EdgeReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        IgniteQuery igniteQuery = table.getAllQuery();
        
        if(limit>0) {
        	igniteQuery.withRange(offset,limit);
        }

        return IgniteResultTransform
                .map(igniteQuery.getResult(),parser::parse);
    }

    public Iterator<Edge> edges(Object fromId, int limit) {
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        final EdgeReader parser = new EdgeReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        IgniteQuery igniteQuery;        
       
        igniteQuery = table.getLimitQuery(fromId, limit);

        return IgniteResultTransform
                .map(igniteQuery.getResult(),parser::parse);
    }
    /**
     * Method to find all edges that refer to the provided
     * vertex that match direction and the provided labels
     */
    public Iterator<Edge> edges(IgniteVertex vertex, Direction direction, String... labels) {
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        final EdgeReader parser = new EdgeReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        IgniteQuery igniteQuery = table.getEdgesQuery(vertex.id(), direction, labels);

        return IgniteResultTransform
                .map(igniteQuery.getResult(),parser::parse);
    }
    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, and a property with
     * a specific value
     */
    public Iterator<Edge> edges(IgniteVertex vertex, Direction direction, String label,
                                String key, Object value) {
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        final EdgeReader parser = new EdgeReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        IgniteQuery igniteQuery = table.getEdgesQuery(vertex.id(), direction, label, key, value);

        return IgniteResultTransform
                .map(igniteQuery.getResult(),parser::parse);
    }

    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, property and a range
     * of property values
     */
    public Iterator<Edge> edgesInRange(IgniteVertex vertex, Direction direction, String label,
                                       String key, Object inclusiveFromValue, Object exclusiveToValue) {
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        final EdgeReader parser = new EdgeReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        IgniteQuery igniteQuery = table.getEdgesInRangeQuery(vertex.id(), direction, label, key,
                inclusiveFromValue, exclusiveToValue);

        return IgniteResultTransform
                .map(igniteQuery.getResult(),parser::parse);
    }
    /**
     * Method to retrieve all edges that refer to the provided
     * vertex and match direction, label, property and a range
     * of property values with a start value and a limit
     */
    public Iterator<Edge> edgesWithLimit(IgniteVertex vertex, Direction direction, String label,
                                         String key, Object fromValue, int limit, boolean reversed) {
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        final EdgeReader parser = new EdgeReader(graph);
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        IgniteQuery igniteQuery = table.getEdgesWithLimitQuery(vertex.id(), direction, label, key,
                fromValue, limit, reversed);

        return IgniteResultTransform
                .map(igniteQuery.getResult(),parser::parse);
    }
    /**
     * Method to retrieve all vertices that refer to the provided
     * vertex that can be reached via related edges
     */
    public Iterator<Vertex> vertices(IgniteVertex vertex, Direction direction, String... labels) {
        Iterator<Edge> edges = edges(vertex, direction, labels);
        return edgesToVertices(vertex, edges);
    }

    public Iterator<Vertex> vertices(IgniteVertex vertex, Direction direction, String label,
                                     String edgeKey, Object edgeValue) {
        Iterator<Edge> edges = edges(vertex, direction, label, edgeKey, edgeValue);
        return edgesToVertices(vertex, edges);
    }

    public Iterator<Vertex> verticesInRange(IgniteVertex vertex, Direction direction, String label,
                                            String edgeKey, Object inclusiveFromEdgeValue, Object exclusiveToEdgeValue) {
        Iterator<Edge> edges = edgesInRange(vertex, direction, label, edgeKey, inclusiveFromEdgeValue, exclusiveToEdgeValue);
        return edgesToVertices(vertex, edges);
    }

    public Iterator<Vertex> verticesWithLimit(IgniteVertex vertex, Direction direction, String label,
                                              String edgeKey, Object fromEdgeValue, int limit, boolean reversed) {
        Iterator<Edge> edges = edgesWithLimit(vertex, direction, label, edgeKey, fromEdgeValue, limit, reversed);
        return edgesToVertices(vertex, edges);
    }

    private Iterator<Vertex> edgesToVertices(IgniteVertex vertex, Iterator<Edge> edges) {
        /*
         * Retrieve the vertex from the respective
         * in or out vertex
         */
        List<Vertex> vertices = Streams.stream(edges).map(edge -> {

            Object inVertexId = edge.inVertex().id();
            Object outVertexId = edge.outVertex().id();

            Object vertexId = vertex.id().equals(inVertexId) ? outVertexId : inVertexId;
            return graph.findOrCreateVertex(vertexId);
        })
        .collect(Collectors.toList());

        graph.getVertexModel().load(vertices);
        return vertices.iterator();

    }

}
