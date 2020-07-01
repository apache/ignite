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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.Dispatcher;
import okhttp3.FormBody;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.db.JdbcQueryExecutor;
import org.apache.ignite.console.agent.handlers.DatabaseListener;
import org.apache.ignite.console.agent.rest.presto.PrestoClient;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.h2.engine.Mode;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.json.JSONArray;
import org.json.JSONException;
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
 * API to translate REST requests to rds use jdbc.
 */
public class RestForPrestoExecutor implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(RestForPrestoExecutor.class));

    /** JSON object mapper. */
    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(true);
    /** */
    private final OkHttpClient httpClient;
    
    private AgentConfiguration cfg;

    /** Index of alive node URI. */
    private final Map<List<String>, Integer> startIdxs = U.newHashMap(2);
    
    int jdbcQueryCancellationTime = 10000;
    
    private PrestoClient presto;

    /**
     * Constructor.
     *    
     * @throws GeneralSecurityException If failed to initialize SSL.
     * @throws IOException If failed to load content of key stores.
     */
    public RestForPrestoExecutor(
    		AgentConfiguration cfg
    ) throws GeneralSecurityException, IOException {
    	
    	this.cfg = cfg;
    	
        Dispatcher dispatcher = new Dispatcher();

        dispatcher.setMaxRequests(Integer.MAX_VALUE);
        dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);

        OkHttpClient.Builder builder = new OkHttpClient.Builder()
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .dispatcher(dispatcher);        

        httpClient = builder.build();
        
       
    }

    /**
     * Stop HTTP client.
     */
    @Override public void close() {
        if (httpClient != null) {
            httpClient.dispatcher().executorService().shutdown();

            httpClient.dispatcher().cancelAll();
        }        
    }

    /** */
    private RestResult parseResponse(String token,String queryId,ArrayNode columns,ArrayNode data,long start) throws IOException {
        if (columns!=null && data!=null) {            
            
            ObjectNode queryResult = new ObjectNode(jsonNodeFactory);
            queryResult.put("rows", data); 
            ArrayNode igniteColumns = new ArrayNode(jsonNodeFactory);
            addMetadata(columns,igniteColumns,columns.size());
            queryResult.put("columns", igniteColumns);
            queryResult.put("protocolVersion", 1);
          
            long end = System.currentTimeMillis();
            queryResult.put("duration", end-start);
            
            ObjectNode result = new ObjectNode(jsonNodeFactory);
            result.put("result", queryResult);
            
            ObjectNode res = new ObjectNode(jsonNodeFactory);
            res.put("result", result);          
            res.put("id", queryId); 
            return RestResult.success(res.toPrettyString(), token);
        }

        return RestResult.fail(STATUS_FAILED, "Failed to execute REST command. ");
    }

    private void addMetadata(ArrayNode resultSetMetaData, ArrayNode metaDataArray,
            int columnCount)  {

		for (int counter = 0; counter < columnCount; counter++) {
			JsonNode node = resultSetMetaData.get(counter);
			ObjectNode object = new ObjectNode(jsonNodeFactory);
			object.put("fieldName", node.get("name").asText());
			
			//object.put("typeName", resultSetMetaData.getTableName(counter));
			//object.put("schemaName", resultSetMetaData.getSchemaName(counter));
			
			String columnType = node.get("type").asText();
			object.put("fieldType", columnType);
			DataType type = DataType.getTypeByName(node.get("typeSignature").get("rawType").asText("varchar").toUpperCase(), Mode.getRegular());
			if(type==null) {
				type = DataType.getDataType(Value.JAVA_OBJECT);
			}
			String aClass = DataType.getTypeClassName(type.type);
			
			object.put("fieldTypeName", aClass);

			metaDataArray.add(object);
		}
		
    }
    /** */
    public RestResult sendRequestToPresto(String url, Map<String, Object> params, Map<String, Object> headers) throws IOException {
    	int pos = url.lastIndexOf('/');
    	String calalog = "";
    	if(pos>10) {
    		calalog = url.substring(pos+1);
    		url = url.substring(0,pos);
    	}
    	String cmd = (String)params.get("p2");
    	if(cmd==null) {
        	cmd = (String)params.get("cmd");
        }
    	presto = new PrestoClient(httpClient,url,calalog);
    	
    	if("org.apache.ignite.internal.visor.cache.VisorCacheNamesCollectorTask".equals(cmd)) {
    		return RestResult.fail(STATUS_FAILED, "Failed to execute REST command: " + presto.$HTTP_error);
    	}
    	else if("metadata".equals(cmd)) {
    		return RestResult.fail(STATUS_FAILED, "Failed to execute REST command: " + presto.$HTTP_error);
    	}
    	else {
	        String schema = (String)params.get("p4");
	        String query = (String)params.get("p5");
	    	
	    	presto.setSchema(schema);
	    	if(headers!=null)
	    		presto.$headers.putAll(headers);
	    	//Execute the request and build the result
	    	boolean rv = presto.Query(query);
	    	if(rv) {
	    		long start = System.currentTimeMillis();
	    		presto.WaitQueryExec();
				//Get the result
				ArrayNode $answer = presto.GetData();
				ArrayNode $cols = presto.GetColumns();
				return parseResponse((String)params.get("token"),presto.$queryId,$cols,$answer,start);
	    	}
	    	else {
	    		return RestResult.fail(STATUS_FAILED, "Failed to execute REST command: " + presto.$HTTP_error);
	    	}
    	 
    	}
    }

    
    public RestResult sendRequest(
            List<String> nodeURIs,
            Map<String, Object> params,
            Map<String, Object> headers
        ) throws ConnectException {
            Integer startIdx = startIdxs.getOrDefault(nodeURIs, 0);

            int urlsCnt = nodeURIs.size();

            for (int i = 0;  i < urlsCnt; i++) {
                Integer currIdx = (startIdx + i) % urlsCnt;

                String nodeUrl = nodeURIs.get(currIdx);

                try {
                    RestResult res = sendRequestToPresto(nodeUrl, params, headers);

                    // If first attempt failed then throttling should be cleared.
                    if (i > 0)
                        LT.clear();

                    LT.info(log, "Connected to cluster [url=" + nodeUrl + "]");

                    startIdxs.put(nodeURIs, currIdx);

                    return res;
                }
                catch (IOException ignored) {
                    LT.warn(log, "Failed connect to cluster [url=" + nodeUrl + "]");
                }
            }

            LT.warn(log, "Failed connect to cluster. " +
                "Please ensure that nodes have [ignite-rest-http] module in classpath " +
                "(was copied from libs/optional to libs folder).");

            throw new ConnectException("Failed connect to cluster [urls=" + nodeURIs + ", parameters=" + params + "]");
        }
    
    

    /**
     * REST response holder Java bean.
     */
    private static class RestResponseHolder {
        /** Success flag */
        private int successStatus;

        /** Error. */
        private String err;

        /** Response. */
        private String res;

        /** Session token string representation. */
        private String sesTok;

        /**
         * @return {@code True} if this request was successful.
         */
        public int getSuccessStatus() {
            return successStatus;
        }

        /**
         * @param successStatus Whether request was successful.
         */
        public void setSuccessStatus(int successStatus) {
            this.successStatus = successStatus;
        }

        /**
         * @return Error.
         */
        public String getError() {
            return err;
        }

        /**
         * @param err Error.
         */
        public void setError(String err) {
            this.err = err;
        }

        /**
         * @return Response object.
         */
        public String getResponse() {
            return res;
        }

        /**
         * @param res Response object.
         */
        @JsonDeserialize(using = RawContentDeserializer.class)
        public void setResponse(String res) {
            this.res = res;
        }

        /**
         * @return String representation of session token.
         */
        public String getSessionToken() {
            return sesTok;
        }

        /**
         * @param sesTok String representation of session token.
         */
        public void setSessionToken(String sesTok) {
            this.sesTok = sesTok;
        }
    }

    /**
     * Raw content deserializer that will deserialize any data as string.
     */
    private static class RawContentDeserializer extends JsonDeserializer<String> {
        /** */
        private final JsonFactory factory = new JsonFactory();

        /**
         * @param tok Token to process.
         * @param p Parser.
         * @param gen Generator.
         */
        private void writeToken(JsonToken tok, JsonParser p, JsonGenerator gen) throws IOException {
            switch (tok) {
                case FIELD_NAME:
                    gen.writeFieldName(p.getText());
                    break;

                case START_ARRAY:
                    gen.writeStartArray();
                    break;

                case END_ARRAY:
                    gen.writeEndArray();
                    break;

                case START_OBJECT:
                    gen.writeStartObject();
                    break;

                case END_OBJECT:
                    gen.writeEndObject();
                    break;

                case VALUE_NUMBER_INT:
                    gen.writeNumber(p.getBigIntegerValue());
                    break;

                case VALUE_NUMBER_FLOAT:
                    gen.writeNumber(p.getDecimalValue());
                    break;

                case VALUE_TRUE:
                    gen.writeBoolean(true);
                    break;

                case VALUE_FALSE:
                    gen.writeBoolean(false);
                    break;

                case VALUE_NULL:
                    gen.writeNull();
                    break;

                default:
                    gen.writeString(p.getText());
            }
        }

        /** {@inheritDoc} */
        @Override public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonToken startTok = p.getCurrentToken();

            if (startTok.isStructStart()) {
                StringWriter wrt = new StringWriter(4096);

                JsonGenerator gen = factory.createGenerator(wrt);

                JsonToken tok = startTok, endTok = startTok == START_ARRAY ? END_ARRAY : END_OBJECT;

                int cnt = 1;

                while (cnt > 0) {
                    writeToken(tok, p, gen);

                    tok = p.nextToken();

                    if (tok == startTok)
                        cnt++;
                    else if (tok == endTok)
                        cnt--;
                }

                gen.close();

                return wrt.toString();
            }

            return p.getValueAsString();
        }
    }
}
