/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.agent.rest;

import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.script.Bindings;
import javax.script.ScriptException;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


/**
 * API to execute REST requests to Ignite cluster.
 */
public class GremlinExecutor implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(GremlinExecutor.class));    
    
    /** */
    private final GremlinGroovyScriptEngine engine;
    
    int gremlinPort = 8182;

    /**
     * @param sslCtxFactory Ssl context factory.
     */
    public GremlinExecutor(int gremlinPort) {
    	engine = new GremlinGroovyScriptEngine();
    	this.gremlinPort = gremlinPort;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
        	engine.reset();
        }
        catch (Throwable e) {
            log.error("Failed to close HTTP client", e);
        }
    }

    private void addFieldMetadata(JsonArray metaDataArray, String name,String type) {
    	JsonObject object = new JsonObject();
		object.put("fieldName", name);
		object.put("typeName", type);
		object.put("schemaName", "default");		
		object.put("fieldTypeName", type);
		metaDataArray.add(object);	
	}
    
    private void addFieldMetadata(JsonArray metaDataArray, Set<String> keys) {
    	for(String key: keys) {
    		addFieldMetadata(metaDataArray, key, "String");
    	}    	
	}
    
    private void addFieldMetadata(JsonArray metaDataArray, Map<String,Object> dict) {    	
    	for(Map.Entry<String,Object> ent: dict.entrySet()) {
    		Class vCls = ent.getValue().getClass();
    		if(vCls.isArray()) {
    			vCls = vCls.getComponentType();
    		}
    		addFieldMetadata(metaDataArray, ent.getKey(), vCls.getSimpleName());   		
    	}
	}
    
    private JsonObject parseResponse(Object object, String nodeId, JsonObject params) throws IOException {
    	JsonObject queryResult = new JsonObject();
        String err = null;
        try {           
           
            int rowCount = 0; //To count the number of rows
            
            queryResult.put("hasMore", false);
            queryResult.put("queryId", 0);
            queryResult.put("responseNodeId", nodeId);            

            JsonArray metaDataArray = new JsonArray();

            JsonArray dataArray = new JsonArray();
            
            if (object instanceof Collection) {
            	Collection list = (Collection) object;
            	for(Object item: list) {
            		JsonArray row = parseObject(item,metaDataArray,rowCount);
                    dataArray.add(row); 
                    
                    ++rowCount;
            	}
            }
            else if (object instanceof Object[]) {
            	Object[] list = (Object[]) object;
            	for(Object item: list) {
            		JsonArray row = parseObject(item,metaDataArray,rowCount);
                    dataArray.add(row); 
                    
                    ++rowCount;
            	}
            }
            else if (object!=null) {
            	JsonArray row = parseObject(object,metaDataArray,rowCount);
                dataArray.add(row);                
                ++rowCount;
            }
            queryResult.put("rows", dataArray); 
            queryResult.put("columns", metaDataArray);
            queryResult.put("protocolVersion", 1);
            return queryResult;
            
        } catch (Exception ex) {
        	err = ex.getMessage();
        	queryResult.put("error",err);        
		}    
        
        return queryResult;        
    }
    
    
    
    private JsonObject parseResponse(ResultSet resultSet,String nodeId,JsonObject params) throws IOException {
    	    
        JsonObject queryResult = new JsonObject();
        String err = null;
        try {           
           
            int rowCount = 0; //To count the number of rows
            
            queryResult.put("hasMore", false);
            queryResult.put("queryId", 0);
            queryResult.put("responseNodeId", nodeId);
            

            JsonArray metaDataArray = new JsonArray();

            JsonArray dataArray = new JsonArray();
            
            for (Result res: resultSet) {
                Object object = res.getObject();
                JsonArray row = parseObject(object,metaDataArray,rowCount);
                dataArray.add(row);
                ++rowCount;
            }
            
            queryResult.put("rows", dataArray); 
            queryResult.put("columns", metaDataArray);
            queryResult.put("protocolVersion", 1);
            return queryResult;
            
        } catch (Exception ex) {
        	err = ex.getMessage();
        	queryResult.put("error",err);        
		}    
        
        return queryResult;
    }
    
    private JsonArray parseObject(Object object, JsonArray metaDataArray,int rowCount) {
    	JsonArray row = new JsonArray();
    	if(object instanceof Vertex) {
        	Vertex v = (Vertex) object;
        	if(rowCount==0) {
        		addFieldMetadata(metaDataArray,"id","String");
        		addFieldMetadata(metaDataArray,"label","String");
        		addFieldMetadata(metaDataArray,v.keys());
        	}                	
        	row.add(v.id());
        	row.add(v.label());
        	for(String key: v.keys()) {
        		row.add(v.value(key));
        	}
        }
        else if(object instanceof Edge) {
        	Edge e = (Edge)object;
        	if(rowCount==0) {
        		addFieldMetadata(metaDataArray,"id","String");
        		addFieldMetadata(metaDataArray,"label","String");
        		addFieldMetadata(metaDataArray,"in","String");
        		addFieldMetadata(metaDataArray,"out","String");
        		addFieldMetadata(metaDataArray,e.keys());
        	}                	
        	row.add(e.id());
        	row.add(e.label());
        	row.add(e.inVertex().id());
        	row.add(e.outVertex().id());
        	for(String key: e.keys()) {
        		row.add(e.value(key));
        	}
        }
        else if(object instanceof VertexProperty) {
        	VertexProperty<Object> vp = (VertexProperty)object;
        	if(rowCount==0) {
        		addFieldMetadata(metaDataArray,"id","String");
        		addFieldMetadata(metaDataArray,"label","String");
        		addFieldMetadata(metaDataArray,"key","String");
        		addFieldMetadata(metaDataArray,"value","String");                		
        	}                	
        	row.add(vp.id());
        	row.add(vp.label());
        	row.add(vp.key());
        	row.add(vp.value());                	
        }
        else if(object instanceof Property) {
        	Property<Object> vp = (Property)object;
        	if(rowCount==0) {                		
        		addFieldMetadata(metaDataArray,"key","String");
        		addFieldMetadata(metaDataArray,"value","String");                		
        	}
        	row.add(vp.key());
        	row.add(vp.value());              	
        }
        else if(object instanceof Map) {
        	Map<String,Object> dict = (Map)object;
        	if(rowCount==0) {                		
        		addFieldMetadata(metaDataArray,dict);
        	}
        	for(Map.Entry<String,Object> ent: dict.entrySet()) {
        		if(ent.getValue() instanceof Object[]) {
        			Object[] list = (Object[])ent.getValue();
        			if(list.length==1) {
        				row.add(list[0]);
        			}
        			else {
        				row.add(ent.getValue());
        			}
        		}
        		else {
        			row.add(ent.getValue());
        		}
        	}
        }
        else {
        	if(rowCount==0) {
        		addFieldMetadata(metaDataArray,"value","Object");
        	}
        	row.add(object);
        }
    	return row;
    }

    /**
     * @param script Groovy script.
     * @param params Request parameters.
     * @return Request result.
     * @throws IOException If failed to parse REST result.
     * @throws Throwable If failed to send request.
     */
    public RestResult sendRequest(Ignite ignite, String clusterId,JsonObject params) throws Throwable {
    	String code = params.getString("qry");
        try {
        	long start = System.currentTimeMillis();    
        	String addr = "localhost";
        	
        	JsonObject res = new JsonObject();
        	res.put("error",(String)null);
        	
        	Cluster cluster = Cluster.build(addr).port(gremlinPort).create();
        	Client client = cluster.connect();        	
        	Map<String,Object> context = new HashMap<>();
        	context.put("ignite_name", ignite.name());
        	ResultSet result = client.submit(code,context);
        	
        	String nodeId = ignite.cluster().localNode().id().toString();
        	JsonObject data = parseResponse(result, nodeId, params);
        	long end = System.currentTimeMillis();
        	data.put("duration", end-start);
        	res.put("result", data);
        	res.put("id", "~"+clusterId);
    		res.put("finished",true);
        	return RestResult.success(res.toString(), params.getString("sessionToken"));
        }
        catch (Exception e) {           
            return RestResult.fail(STATUS_FAILED, "Failed to execute Groovy command [code=" +
                    code + ", msg=" + e.getCause().getMessage() + "]");
        }
    }
    
    /**
     * @param script Groovy script.
     * @param params Request parameters.
     * @return Request result.
     * @throws IOException If failed to parse REST result.
     * @throws Throwable If failed to send request.
     */
    public RestResult execRequest(Ignite ignite, Vertx vertx, String clusterId, JsonObject params) throws Throwable {
    	String code = params.getString("qry");
        try {
        	JsonObject res = new JsonObject();
        	res.put("error",(String)null);
        	long start = System.currentTimeMillis();    
        	Bindings bindings = engine.createBindings();
        	bindings.put("vertx",vertx);
        	bindings.put("ignite",ignite);
        	
        	Object result = engine.eval(code, bindings);
        	String nodeId = ignite.cluster().localNode().id().toString();
        	JsonObject data = parseResponse(result, nodeId, params);
            long end = System.currentTimeMillis();
        	data.put("duration", end-start);
        	res.put("result", data);
        	res.put("id", "~"+clusterId);
    		res.put("finished",true);
    		return RestResult.success(res.toString(), params.getString("sessionToken"));
        }
        catch (ScriptException e) {           
            return RestResult.fail(STATUS_FAILED, "Failed to execute Groovy command [code=" +
                    code + ", msg=" + e.getCause().getMessage() + "]");
        }
    }
}
