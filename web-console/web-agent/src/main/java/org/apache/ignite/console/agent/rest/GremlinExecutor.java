

package org.apache.ignite.console.agent.rest;

import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.script.Bindings;
import javax.script.ScriptException;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.service.LangflowApiClient;
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
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
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
    
    TypeSerializerRegistry typeSerializerRegistry;
    
    GraphBinaryMessageSerializerV1 gbSerializer;
    
    private String gremlinAddr = "localhost";
    private int gremlinPort = 8182;
    
    private String prevURL = "";
    
    private Client client;
    
    

    /**
     * @param sslCtxFactory Ssl context factory.
     */
    public GremlinExecutor(String addr, int gremlinPort) {
    	engine = new GremlinGroovyScriptEngine();
    	if(addr!=null) {
	    	try {
				URL url = new URL(addr);
				this.gremlinAddr = url.getHost();
			} catch (MalformedURLException e) {
		    	this.gremlinAddr = addr;
			}
    	}
    	this.gremlinPort = gremlinPort;    	
    	
    	try {
    		Class<? extends IoRegistry> ioReg = (Class)Class.forName("org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry");
    		Constructor<? extends IoRegistry> cons = ioReg.getDeclaredConstructor();
    		cons.setAccessible(true);
    		
			typeSerializerRegistry = TypeSerializerRegistry.build()
				    .addRegistry(cons.newInstance())
				    .create();
			
			gbSerializer = new GraphBinaryMessageSerializerV1(typeSerializerRegistry);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | SecurityException | IllegalArgumentException | InvocationTargetException e) {
			log.info("constuct typeSerializerRegistry " + e.getMessage());
			gbSerializer = new GraphBinaryMessageSerializerV1();
		}
    		
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

    private static void addFieldMetadata(JsonArray metaDataArray, String name,String type) {
    	JsonObject object = new JsonObject();
    	object.put("schemaName", "default");
		object.put("fieldName", name);
		object.put("fieldTypeName", type);
		metaDataArray.add(object);	
	}
    
    private static void addFieldMetadata(JsonArray metaDataArray, Set<String> keys) {
    	for(String key: keys) {
    		addFieldMetadata(metaDataArray, key, "String");
    	}    	
	}
    
    private static void addFieldMetadata(JsonArray metaDataArray, Map<String,Object> dict) {    	
    	for(Map.Entry<String,Object> ent: dict.entrySet()) {
    		Class vCls = ent.getValue()==null? Object.class: ent.getValue().getClass();
    		if(vCls.isArray()) {
    			vCls = vCls.getComponentType();
    		}
    		addFieldMetadata(metaDataArray, ent.getKey(), vCls.getSimpleName());   		
    	}
	}
    
    public static JsonObject parseResponse(Object object, String nodeId, JsonObject params) throws IOException {
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
    
    public static JsonObject parseKeyValueResponse(JsonObject dataObject, String nodeId) throws IOException {
    	JsonObject queryResult = new JsonObject();
        String err = null;
        try {           
           
            int rowCount = 0; //To count the number of rows
            
            queryResult.put("hasMore", false);
            queryResult.put("queryId", 0);
            queryResult.put("responseNodeId", nodeId);            
            queryResult.put("protocolVersion", 1);
            
            JsonArray metaDataArray = new JsonArray();            

            JsonArray dataArray = new JsonArray();
            
            JsonArray items = dataObject.getJsonArray("items");
    		
    		for(int i=0;i<items.size();i++) {
    			JsonObject node = items.getJsonObject(i);
    			Object value = node.getValue("value");
    			if(value instanceof JsonObject) {
    				JsonObject jsonValue = (JsonObject) value;
    				JsonArray row = new JsonArray();
    				row.add(node.getValue("key"));
    				if(rowCount==0) {
    					addFieldMetadata(metaDataArray,"_key",node.getValue("key").getClass().getSimpleName());
    				}
    				for(Map.Entry<String, Object> ent: jsonValue.getMap().entrySet()) {
    					Object v = ent.getValue();
    					if(rowCount==0) {
            				JsonObject object = new JsonObject();
            				object.put("schemaName", "default");
            				object.put("fieldName", ent.getKey());
            				object.put("fieldTypeName",v==null? "Object": v.getClass().getSimpleName());
            				metaDataArray.add(object);	
        				}
    					row.add(v);
    				}
    				rowCount++;
    				dataArray.add(row);
    			}
    		}
    		if(items.size()>0) {
	            queryResult.put("rows", dataArray); 
	            queryResult.put("columns", metaDataArray);
	            
	            return queryResult;
    		}
            
        } catch (Exception ex) {
        	err = ex.getMessage();
        	queryResult.put("error",err);        
		}    
        
        return queryResult;        
    }
    
    public static JsonObject parseResponse(ResultSet resultSet,String nodeId,JsonObject params) throws IOException {
    	    
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
    
    private static JsonArray parseObject(Object object, JsonArray metaDataArray,int rowCount) {
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
        else if(object instanceof JsonObject) {
        	Map<String,Object> dict = ((JsonObject)object).getMap();
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
    public RestResult sendRequest(Map<String,Object> context, String clusterId, JsonObject params) throws Throwable {
    	String code = params.getString("qry");
        try {
        	long start = System.currentTimeMillis();
        	
        	JsonObject res = new JsonObject();
        	res.put("error",(String)null);
        	
        	if(!this.prevURL.equals(clusterId) || this.client.isClosing()) {
        		
        		Cluster cluster = Cluster.build()
        	    	    .addContactPoint(gremlinAddr)
        	    	    .port(gremlinPort)
        	    	    .serializer(gbSerializer)
        	    	    .maxConnectionPoolSize(4)
        	    	    .maxInProcessPerConnection(1)
        	    	    .maxSimultaneousUsagePerConnection(4)
        	    	    .workerPoolSize(8)        	    	   
        	    	    .create();
                	
                this.client = cluster.connect();
        		
        		this.prevURL = clusterId;
        		
        	}
        	
        	
        	ResultSet result = client.submit(code,context);

        	JsonObject data = parseResponse(result, clusterId, params);
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
