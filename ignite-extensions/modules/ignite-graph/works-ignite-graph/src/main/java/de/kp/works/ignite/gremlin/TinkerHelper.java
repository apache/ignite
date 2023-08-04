/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package de.kp.works.ignite.gremlin;

import org.apache.ignite.Ignite;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import de.kp.works.ignite.gremlin.process.computer.TinkerGraphComputerView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerHelper {

    private TinkerHelper() {
    }    

    public static Ignite ignite(final IgniteGraph graph) {
    	return graph.admin.ignite();
    }
    
    public static boolean inComputerMode(final IgniteGraph graph) {
        return null != graph.graphComputerView;
    }

    public static TinkerGraphComputerView createGraphComputerView(final IgniteGraph graph, final GraphFilter graphFilter, final Set<VertexComputeKey> computeKeys) {
        return graph.graphComputerView = new TinkerGraphComputerView(graph, graphFilter, computeKeys);
    }

    public static TinkerGraphComputerView getGraphComputerView(final IgniteGraph graph) {
        return graph.graphComputerView;
    }

    public static void dropGraphComputerView(final IgniteGraph graph) {
        graph.graphComputerView = null;
    }

    public static Map<String, List<VertexProperty>> getProperties(final IgniteVertex vertex) {
        if(null == vertex.properties) return Collections.emptyMap();
        Map<String, List<VertexProperty>> properties = new HashMap<>();
        for(var entry: vertex.properties.entrySet()) {
        	List<VertexProperty> items = properties.computeIfAbsent(entry.getKey(),s-> new ArrayList<VertexProperty>(1));
        	
        	items.add(new IgniteVertexProperty(vertex,entry.getKey(),entry.getValue()));        	
        }        
        return properties;
    }

    

    /**
     * Search for {@link Property}s attached to {@link Element}s of the supplied element type using the supplied
     * regex. This is a basic scan+filter operation, not a full text search against an index.
     */
    public static <E extends Element> Iterator<Property> search(final IgniteGraph graph, final String regex,
                                                                final Optional<Class<E>> type) {

        final Supplier<Iterator<Element>> vertices = () -> IteratorUtils.cast(graph.vertices());
        final Supplier<Iterator<Element>> edges = () -> IteratorUtils.cast(graph.edges());
        final Supplier<Iterator<Element>> vertexProperties =
                () -> IteratorUtils.flatMap(vertices.get(), v -> IteratorUtils.cast(v.properties()));

        Iterator it;
        if (!type.isPresent()) {
            it = IteratorUtils.concat(vertices.get(), edges.get(), vertexProperties.get());
        } 
        else 
        	switch (type.get().getSimpleName()) {
	            case "Edge":
	                it = edges.get();
	                break;
	            case "Vertex":
	                it = vertices.get();
	                break;
	            case "VertexProperty":
	                it = vertexProperties.get();
	                break;
	            default:
	                it = IteratorUtils.concat(vertices.get(), edges.get(), vertexProperties.get());
	        }

        final Pattern pattern = Pattern.compile(regex);

        // get properties
        it = IteratorUtils.<Element, Property>flatMap(it, e -> IteratorUtils.cast(e.properties()));
        // filter by regex
        it = IteratorUtils.<Property>filter(it, p -> pattern.matcher(p.value().toString()).matches());

        return it;
    }

    /**
     * Search for {@link Property}s attached to any {@link Element} using the supplied regex. This
     * is a basic scan+filter operation, not a full text search against an index.
     */
    public static Iterator<Property> search(final IgniteGraph graph, final String regex) {
        return search(graph, regex, Optional.empty());
    }

}
