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

import de.kp.works.ignite.query.IgniteQuery;
import de.kp.works.ignite.query.IgniteResult;
import de.kp.works.ignite.IgniteResultTransform;
import de.kp.works.ignite.IgniteTable;
import de.kp.works.ignite.ValueUtils;
import de.kp.works.ignite.gremlin.IgniteDocument;
import de.kp.works.ignite.gremlin.IgniteElement;
import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.gremlin.IgniteVertex;
import de.kp.works.ignite.gremlin.exception.IgniteGraphNotFoundException;
import de.kp.works.ignite.gremlin.mutators.*;
import de.kp.works.ignite.gremlin.readers.VertexReader;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class VertexModel extends ElementModel {

    public VertexModel(IgniteGraph graph, IgniteTable table) {
        super(graph, table);
    }

    public VertexReader getReader() {
        return new VertexReader(graph);
    }
    
    /**
     * Load the elements from the backing table.
     *
     * @param elements The elements
     */
    @SuppressWarnings("unchecked")
    public void load(List<? extends Element> elements) {
        LOGGER.trace("Executing Batch Get, type: {}", getClass().getSimpleName());
        List<IgniteDocument> docs = new LinkedList<>();
        final List<Vertex> vertices = new ArrayList<>(elements.size());
        List<Object> ids = elements.stream()
    		.filter( vertex -> {
    			// 已经加载了
    			if(vertex instanceof IgniteElement && ((IgniteElement)vertex).arePropertiesFullyLoaded()) {
    				return false;
    			}
    			if (vertex instanceof IgniteDocument) {
                	docs.add((IgniteDocument)vertex);
                	return false;
                }
    			vertices.add((Vertex)vertex);
    			return true;
    		})
            .map(Element::id)            
            .collect(Collectors.toList());
        
        if(ids.size()>0) {
	        List<IgniteResult> results = table.get(ids);
	        for (int i = 0; i < results.size(); i++) {
	            try {
	                getReader().load(vertices.get(i), results.get(i));
	            } catch (IgniteGraphNotFoundException e) {
	                // ignore, the element will not have its properties fully loaded
	            }
	        }
        }
        
        if(docs.size()>0) {
        	docs.forEach( doc -> {        		
            	doc.load();         	
            });
        }
    }
    
    /** WRITE & DELETE **/

    public void writeVertex(Vertex vertex) {
        Creator creator = new VertexWriter(graph, vertex);
        Mutators.create(table, creator);
    }

    public void deleteVertex(Vertex vertex) {
        Mutator writer = new VertexRemover(graph, vertex);
        Mutators.write(table, writer);
    }

    /** READ **/

    public Iterator<Vertex> vertices(int offset, int limit) {
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        VertexReader parser = getReader();
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

    public Iterator<Vertex> vertices(Object fromId, int limit) {
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        final VertexReader parser = getReader();
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
     * This method retrieves all vertices that refer to
     * the same label.
     */
    public Iterator<Vertex> vertices(String label,int offset, int limit) {
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        VertexReader parser = getReader();
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        IgniteQuery igniteQuery = table.getLabelQuery(label);
        if(limit>0) {
        	igniteQuery.withRange(offset,limit);
        }

        return IgniteResultTransform
                .map(igniteQuery.getResult(),parser::parse);
    }

    /**
     * This method retrieves all vertices that refer
     * to a certain label, property key and value
     */
    public Iterator<Vertex> vertices(String label, String key, Object value) {
        ElementHelper.validateProperty(key, value);
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        VertexReader parser = getReader();
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        IgniteQuery igniteQuery = table.getPropertyQuery(label, key, value);

        return IgniteResultTransform
                .map(igniteQuery.getResult(),parser::parse);
    }

    public Iterator<Vertex> verticesInRange(String label, String key, Object inclusiveFrom, Object exclusiveTo) {

        ElementHelper.validateProperty(key, inclusiveFrom);
        ElementHelper.validateProperty(key, exclusiveTo);
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        VertexReader parser = getReader();
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        IgniteQuery igniteQuery = table.getRangeQuery(label, key, inclusiveFrom, exclusiveTo);

        return IgniteResultTransform
                .map(igniteQuery.getResult(),parser::parse);
    }

    public Iterator<Vertex> verticesWithLimit(String label, String key, Object from, int limit, boolean reversed) {

        ElementHelper.validateProperty(key, from != null ? from : new Object());
        /*
         * The parser converts results from Ignite
         * queries to vertices.
         */
        VertexReader parser = getReader();
        /*
         * The query is responsible for retrieving the
         * requested vertices from the Ignite cache.
         */
        IgniteQuery igniteQuery = table.getLimitQuery(label, key, from, limit, reversed);

        return IgniteResultTransform
                .map(igniteQuery.getResult(),parser::parse);
    }
}
