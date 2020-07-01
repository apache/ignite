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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;


import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.Dispatcher;
import okhttp3.FormBody;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.db.JdbcQueryExecutor;
import org.apache.ignite.console.agent.handlers.DatabaseListener;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.LoggerFactory;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static org.apache.ignite.console.agent.AgentUtils.sslConnectionSpec;
import static org.apache.ignite.console.agent.AgentUtils.sslSocketFactory;
import static org.apache.ignite.console.agent.AgentUtils.trustManager;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_AUTH_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/**
 * API to translate REST requests to rds use jdbc connect.
 */
public class RestForRDSExecutor implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(RestForRDSExecutor.class));
   
    
    private DatabaseListener dbListener;

    /** Index of alive node URI. */
    private final Map<List<String>, Integer> startIdxs = U.newHashMap(2);
    
    int jdbcQueryCancellationTime = 10000;

    /**
     * Constructor.
     *    
     * @throws GeneralSecurityException If failed to initialize SSL.
     * @throws IOException If failed to load content of key stores.
     */
    public RestForRDSExecutor(
    		DatabaseListener dbListener
    ) throws GeneralSecurityException, IOException {
    	
    	this.dbListener = dbListener;
    	
    }

    /**
     * Stop jdbc client.
     */
    @Override public void close() {
       
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
    public RestResult sendRequest(Map<String, Object> args, 
        Map<String, Object> params,
        Map<String, Object> headers
    ) throws IOException {
    	
    	String jdbcDriverCls = this.dbListener.currentDriverCls;
    	String jdbcUrl = this.dbListener.currentJdbcUrl;
    	Properties jdbcInfo = this.dbListener.currentJdbcInfo;
        
    	String nodeUrl = this.dbListener.currentJdbcUrl;
    	if(jdbcUrl==null) {
    		return RestResult.fail(STATUS_FAILED, "Not configure any jdbc connection, Please click Import from Database on configuration/overview");
    	}
    	String cmd = (String)params.get("p2");
    	if(cmd==null) {
        	cmd = (String)params.get("cmd");
        }
    	Connection conn = null;
    	int  urlsCnt = 1;
        for (int i = 0;  i < urlsCnt; i++) {           

            try {
            	conn = dbListener.connect(null, jdbcDriverCls, jdbcUrl, jdbcInfo);
            	JSONObject res;
            	if("org.apache.ignite.internal.visor.cache.VisorCacheNamesCollectorTask".equals(cmd)) {
            		DatabaseMetaData metadata = conn.getMetaData();                    
            		ResultSet tables = metadata.getTables(
                    		conn.getCatalog(),
                            null,
                            null,
                            new String[] {"TABLE", "VIEW"});
            		res = new JSONObject();
                    JSONObject result = new JSONObject();
                    JSONObject caches = new JSONObject();
            		
                    while (tables.next()) {
                    	//caches.put(tables.getString("TABLE_NAME"),tables.getString("TABLE_SCHEM"));
                    	Object schema = tables.getObject("TABLE_SCHEM");
                    	caches.put(schema.toString(),tables.getString("TABLE_SCHEM"));
                    }
                    result.put("caches", caches);
                    result.put("protocolVersion", 1);
                    res.put("result", result);
            	}
            	else if("metadata".equals(cmd)) {
            		DatabaseMetaData metadata = conn.getMetaData();                    
            		ResultSet tables = metadata.getTables(
                    		conn.getCatalog(),
                            null,
                            null,
                            new String[] {"TABLE", "VIEW"});
            		JSONArray arr = new JSONArray();            		
                    while (tables.next()) {
                    	JSONObject caches = new JSONObject();
                    	String typeName = tables.getObject("TABLE_SCHEM")+"."+tables.getString("TABLE_NAME");
                    	caches.put("cacheName",typeName);
                    	
                    	JSONArray types = new JSONArray();  
                    	types.put(typeName);
                    	caches.put("types",types);
                    	JSONObject fields = new JSONObject();
                    	JSONObject column = new JSONObject();
                    	column.put("ID", "java.lang.Integer");
                    	column.put("NAME", "java.lang.String");
                    	fields.put(typeName, column);
                    	/**
                   "fields": {
						"SQL_PUBLIC_PERSON_e7361252_ba66_4feb_991c_81b866eb4adb": {
							"ID": "java.lang.Integer",
							"NAME": "java.lang.String"
						}
					},
                    	 */
                    	caches.put("fields",fields); 
                    	JSONObject indexes = new JSONObject();
                    	JSONArray index = new JSONArray();  
                    	indexes.put(typeName, index);
                    	caches.put("indexes",indexes);
                    	
                    	//caches.put("keyClasses",fields);
                    	//caches.put("valClasses",fields);
                    	arr.put(caches);
                    }
                   
                    return RestResult.success(arr.toString(), (String)args.get("token"));
            	}
            	else {
            		JdbcQueryExecutor exec = new JdbcQueryExecutor(conn.createStatement(),(String)params.get("p5"));
            	
            		res = exec.executeSqlVisor(0,(String)params.get("p1"));
            		
            	}
            	
            	res.put("id", params.get("p1"));
        		res.put("finished",true);

                // If first attempt failed then throttling should be cleared.
                if (i > 0)
                    LT.clear();

                LT.info(log, "Connected to cluster [url=" + nodeUrl + "]");

                conn.close();
               
                return RestResult.success(res.toString(), (String)args.get("token"));
           
            } catch (SQLException e) {			
            	
				LT.warn(log, "Failed connect to db [url=" + nodeUrl + "] "+e.getMessage());
				return RestResult.fail(STATUS_FAILED, e.getMessage());
			} catch (Exception e) {
				LT.warn(log, "Failed connect to db [url=" + nodeUrl + "] "+e.getMessage());
				return RestResult.fail(STATUS_FAILED, e.getClass().getName() + ": " + e.getMessage());
			}
        }
        
        
        LT.warn(log, "Failed connect to cluster. " +
            "Please ensure that db driver jar in classpath " +
            "(was copied from libs/optional to libs folder).");

        throw new ConnectException("Failed connect to rds [url=" + nodeUrl + ", parameters=" + params + "]");
    }    
}
