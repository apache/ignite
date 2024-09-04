/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.agent.rest;

import java.io.IOException;
import java.io.StringWriter;
import java.net.ConnectException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.db.DbColumn;
import org.apache.ignite.console.agent.db.DbMetadataReader;
import org.apache.ignite.console.agent.db.DbTable;
import org.apache.ignite.console.agent.db.JdbcQueryExecutor;
import org.apache.ignite.console.agent.handlers.DatabaseListener;
import org.apache.ignite.console.db.DBInfo;
import org.apache.ignite.console.db.VisorQueryIndex;
import org.apache.ignite.console.db.VisorQueryIndexField;

import org.apache.ignite.console.utils.Utils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.util.StringUtil;
import org.h2.message.DbException;
import org.h2.value.DataType;

import org.slf4j.LoggerFactory;

import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_AUTH_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/**
 * API to translate REST requests to rds use jdbc connect.
 */
public class JdbcExecutor implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(JdbcExecutor.class));
    public static final JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(true);
    
    private DatabaseListener dbListener;  
    
    private int jdbcQueryCancellationTime = 10000;
    
    private DbMetadataReader metadataReader = new DbMetadataReader();

    /**
     * Constructor.
     *    
     * @throws GeneralSecurityException If failed to initialize SSL.
     * @throws IOException If failed to load content of key stores.
     */
    public JdbcExecutor(
    		DatabaseListener dbListener
    ) {
    	
    	this.dbListener = dbListener;
    	
    }

    /**
     * Stop jdbc client.
     */
    @Override public void close() {
    	dbListener.clear();
    }

    
    /**
     * Send request to cluster.
     *
     * @param nodeURIs List of cluster nodes URIs.
     * @param params Map with reques params.
     * @param headers Map with reques headers.
     * @return Response from cluster.
     * @throws IOException If failed to send request to cluster.
     */
    public RestResult sendRequest(String clusterId, JsonObject args, JsonObject params) throws IOException {
    	 
    	DBInfo dbInfo = dbListener.getDBClusterInfo(clusterId);
    	if(dbInfo==null || dbInfo.jdbcUrl==null) {
    		return RestResult.fail(STATUS_FAILED, "Not configure any jdbc connection, Please click Import from Database on configuration/overview");
    	}
    	if(StringUtil.isBlank(dbInfo.jndiName)) {
    		return RestResult.fail(STATUS_FAILED, "Not configure jdbc jndi name, Please click Import from Database on configuration/overview");
    	}
    	
    	String nodeUrl = dbInfo.jdbcUrl;
    	String cmd = params.getString("cmd");
    	String p2 = params.getString("p2");
    	
    	boolean importSamples = args.getBoolean("importSamples", false);
        	
    	
    	int  urlsCnt = 1;
        for (int i = 0;  i < urlsCnt; i++) { 
        	Connection  conn = null;
            try {            	
            	
            	conn = dbListener.getConnection(dbInfo);
            	if(conn==null) {
            		conn = metadataReader.connect(dbInfo.driverJar, dbInfo);
            	}
            	
            	JsonObject res = new JsonObject();
            	res.put("error",(String)null);
            	
            	if("org.apache.ignite.internal.visor.cache.VisorCacheNamesCollectorTask".equals(p2)) {
            		
            		Collection<String> schemas = metadataReader.schemas(conn,importSamples);
            		
            		JsonObject result = new JsonObject();
            		JsonObject caches = new JsonObject();
            		
                    for (String schema: schemas) {                    
                    	caches.put(schema,schema);
                    }
                    result.put("caches", caches);
                    result.put("groups", Lists.newArrayList());
                    result.put("protocolVersion", 1);
                    res.put("result", result);
                   
            	}
            	else if("org.apache.ignite.internal.visor.cache.VisorCacheNodesTask".equals(p2)) {
            		
                    JsonArray result = new JsonArray();
                    result.add(clusterId);
                    res.put("result", result);
            	}
            	else if("top".equals(cmd)) {
            		
            		JsonArray result = new JsonArray();
            		JsonObject node = new JsonObject();
            		node.put("nodeId", dbInfo.top.getId());
            		node.put("tcpAddresses", dbInfo.jdbcUrl);
            		node.put("consistentId", dbInfo.getJndiName());
            		node.put("tcpPort", dbInfo.getSchemaName());
            		node.put("replicaCount", dbInfo.top.getNodes().size());            		
            		node.put("isActive", dbInfo.top.isActive());            		
            		if(params.getBoolean("attr",false)) {
            			node.put("attributes", dbInfo.getJdbcProp());
            			JsonObject attrs = node.getJsonObject("attributes");
            			attrs.put("database.driverCls", dbInfo.driverCls);
	            		System.getenv().forEach((k,v)->{
	            			if(k.startsWith("ignite") || v.length()<128) {
	            				attrs.put(k, v);
	            			}
	            		});
            		}
            		
                    result.add(node);
                    return RestResult.success(result.toString(), args.getString("sessionToken"));
                    
            	}
            	else if("metadata".equals(cmd)) {
            		
            		List<String> schemas = new ArrayList<>();
            		String schema = params.getString("cacheName");
            		if(!StringUtil.isEmpty(schema)) {
            			schemas.add(schema);
            		}
            		else {
            			schemas.addAll(metadataReader.schemas(conn,importSamples));
            		}
            		
            		Collection<DbTable> meta = metadataReader.cachedMetadata(conn, schemas, false);
            		
            		ArrayNode arr = new ArrayNode(jsonNodeFactory);   
            		ObjectNode cachesMap = new ObjectNode(jsonNodeFactory);
                    for (DbTable table: meta) {
                    	ObjectNode caches = cachesMap.withObject("/"+table.getSchema());
                    	if(caches.isEmpty()) {
                    		caches.put("cacheName",table.getSchema());
                    		arr.add(caches);
                    	}
                    	
                    	String typeName = table.getTable();
                    	                    	
                    	ArrayNode types = caches.withArray("/types");  
                    	types.add(typeName);
                    	
                    	ObjectNode fields = caches.withObject("/fields");
                    	ObjectNode column = new ObjectNode(jsonNodeFactory);
                    	
                    	for(DbColumn col: table.getColumns()) {
                    		String aClass = "java.lang.Object";
                    		try {
                    			aClass = DataType.getTypeClassName(DataType.convertSQLTypeToValueType(col.getType()));
                    		}
                    		catch(DbException e) {
                    			log.warning(e.getMessage());
                    		}
                    		column.put(col.getName(), col.getComment()!=null? aClass+" //"+col.getComment(): aClass);
                    	}                    	
                    	fields.set(typeName, column);
                    	
                    	ObjectNode comments = caches.withObject("/comments");
                    	comments.put(typeName, table.getComment());
                    	
                    	ObjectNode indexes = caches.withObject("/indexes");
                    	ArrayNode index = new ArrayNode(jsonNodeFactory);  
                    	
                    	for(VisorQueryIndex idx: table.getIndexes()) {
                    		ObjectNode indexItem = new ObjectNode(jsonNodeFactory);
                    		index.add(indexItem);
                    		indexItem.put("name",idx.getName());
                    		indexItem.put("unique",false);
                    		indexItem.put("descendings","");
                    		for(VisorQueryIndexField idxField: idx.getFields()) {                    			
                    			indexItem.withArray("/fields").add(idxField.getName());                    			
                    		}
                    		
                    	}
                    	indexes.set(typeName, index);                    	
                    	
                    	//caches.put("keyClasses",fields);
                    	//caches.put("valClasses",fields);
                    	
                    }
                    
                    return RestResult.success(arr.toString(), args.getString("token"));
            	}
            	else if("qryfldexe".equals(cmd)){
            		
            		String schema = params.getString("cacheName");
            		if(!StringUtil.isEmpty(schema)) {
            			conn.setSchema(schema);
            		}
            		JdbcQueryExecutor exec = new JdbcQueryExecutor(conn.createStatement(),params.getString("qry"),schema);
            	
            		JsonObject result = exec.call(0, clusterId);
            		res.put("result", result);
            		
            	}
            	else {
            		res.put("error", "Unsupport cmd "+ cmd);
            	}
            	
            	res.put("id", "~"+clusterId);
        		res.put("finished",true);

                // If first attempt failed then throttling should be cleared.
                if (i > 0)
                    LT.clear();

                LT.info(log, "Connected to cluster [url=" + nodeUrl + "]");                
               
                return RestResult.success(res.toString(), args.getString("sessionToken"));
           
            } catch (SQLException e) {			
            	
				LT.warn(log, "Failed connect to db [url=" + nodeUrl + "] "+e.getMessage());
				return RestResult.fail(STATUS_FAILED, e.getMessage());
			} catch (Exception e) {
				LT.warn(log, "Failed connect to db [url=" + nodeUrl + "] "+e.getMessage());
				return RestResult.fail(STATUS_FAILED, e.getClass().getName() + ": " + e.getMessage());
			}
            finally {
            	if(conn!=null) {
            		try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
            }
        }
        
        
        LT.warn(log, "Failed connect to cluster. " +
            "Please ensure that db driver jar in classpath " +
            "(was copied from libs/optional to libs folder).");

        throw new ConnectException("Failed connect to rds [url=" + nodeUrl + ", parameters=" + params + "]");
    }    
}
