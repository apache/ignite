// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.example;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.SchemaManager;
import org.janusgraph.util.system.ConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class RemoteGraphUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteGraphUtil.class);    
   
    
    public static void copy(GraphTraversalSource fromG,GraphTraversalSource toG) {
    	GraphTraversalSource g = fromG;
    	Traversal<Vertex, Vertex> t = g.V();//.limit(100);
    	while(t.hasNext()) {
	    	Vertex v = t.next();
	    	GraphTraversal<Vertex, Map<Object, Object>> pMap = g.V(v.id()).valueMap();
	    	
	    	Iterator<Entry<Object, Object>> pit = pMap.next().entrySet().iterator();	    	
	    	
	    	GraphTraversal<Vertex, Vertex> pAdder = toG.addV(v.label()).property(T.id, v.id());
	    	while(pit.hasNext()) {
	    		Entry<Object, Object> p = pit.next();
	    		if(p.getValue() instanceof List) {
	    			List array = (List) p.getValue();
	    			if(array.size()==1) {
	    				pAdder.property(p.getKey(), array.get(0));
	    				continue;
	    			}
	    			else if(array.size()==0) {
	    				pAdder.property(p.getKey(), "");
	    				continue;
	    			}
	    		}
	    		pAdder.property(p.getKey(), p.getValue());
	    	}
	    	
	    	Vertex toV = pAdder.next();
	    	
	    	System.out.println(toV);
    	}
    	
    	Traversal<Edge, Edge> r = g.E();
    	while(r.hasNext()) {
	    	Edge e = r.next();
	    	GraphTraversal<Edge, Map<Object, Object>> pMap = g.E(e.id()).valueMap();
	    	
	    	Iterator<Entry<Object, Object>> pit = pMap.next().entrySet().iterator();
	    	
	    	GraphTraversal<Vertex, Edge> pAdder = toG.V(e.inVertex().id()).as("a").V(e.outVertex().id()).addE(e.label()).from("a").property(T.id, e.id());
	    	
	    	while(pit.hasNext()) {
	    		Entry<Object, Object> p = pit.next();
	    		if(p.getValue() instanceof List) {
	    			List array = (List) p.getValue();
	    			if(array.size()==1) {
	    				pAdder.property(p.getKey(), array.get(0));
	    				continue;
	    			}
	    			else if(array.size()==0) {
	    				pAdder.property(p.getKey(), "");
	    				continue;
	    			}
	    		}
	    		pAdder.property(p.getKey(), p.getValue());
	    	}
	    	
	    	Edge toE = pAdder.next();
	    	
	    	System.out.println(toE);
    	}
    	
    }

    public static void main(String[] args) throws ConfigurationException, IOException{
        String hugeFileName = (args != null && args.length > 0) ? args[0] : "conf/remote-huge-graph.properties";
        // 将数据从jana导入到ignite-graph
        final RemoteGraphApp app = new RemoteGraphApp(hugeFileName);
        app.supportsSchema = false;
        app.supportsGeoshape = false;
        app.openGraph();        
        
        String fileName = (args != null && args.length > 0) ? args[0] : "conf/remote-graph.properties";
        RemoteGraphApp toApp = new RemoteGraphApp(fileName);
        
        toApp.openGraph();
        
        copy(app.g,toApp.g);
    }
}
