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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.service.LangflowApiClient;
import org.apache.ignite.console.agent.service.ServiceResult;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;


import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_AUTH_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;

/**
 * API to execute REST requests to Ignite cluster.
 */
public class RestExecutor implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(RestExecutor.class));  
    
    private ConcurrentMap<String, JsonObject> cacheMap = new ConcurrentHashMap<>(16);
    
    /** */
    private final HttpClient httpClient;
    
    private final LangflowApiClient langflowClient;

    /**
     * @param sslCtxFactory Ssl context factory.
     */
    public RestExecutor(SslContextFactory sslCtxFactory) {
        httpClient = HttpClient.newBuilder()
        		.connectTimeout(Duration.ofSeconds(60))
        		.followRedirects(Redirect.NEVER)
        		.build();
        
        langflowClient = new LangflowApiClient();
       
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            //httpClient.stop();
        }
        catch (Throwable e) {
            log.error("Failed to close HTTP client", e);
        }
    }

    /**
     * @param res represents a HTTP response.
     * @param in Returns an {@link InputStream} providing the response content bytes.
     * @return Result of REST request.
     * @throws IOException If failed to parse REST result.
     */
    private RestResult parseResponse(HttpResponse<String> res) throws IOException {
        int code = res.statusCode();

        if (code == HTTP_OK)
            return fromJson(res.body(), RestResult.class);

        if (code == HTTP_UNAUTHORIZED) {
            return RestResult.fail(STATUS_AUTH_FAILED, "Failed to authenticate in cluster. " +
                "Please check agent\'s login and password or node port.");
        }

        if (code == HTTP_NOT_FOUND)
            return RestResult.fail(STATUS_FAILED, "Failed connect to cluster.");

        return RestResult.fail(STATUS_FAILED, "Failed to execute REST command [code=" +
            code + ", msg=" + res.body() + "]");
    }

    /**
     * @param url Request URL.
     * @param params Request parameters.
     * @return Request result.
     * @throws IOException If failed to parse REST result.
     * @throws Throwable If failed to send request.
     */
    public RestResult sendRequest(String url, JsonObject params) throws Throwable {
        
    	final StringBuilder fields = new StringBuilder();
    	
        params.getMap().forEach((k, v) ->{ 
        	if(v!=null) {
        		if(fields.length()>0) {
        			fields.append("&");
        		}        		     		
        		fields.append(k);
        		fields.append("=");
        		fields.append(URLEncoder.encode(String.valueOf(v),StandardCharsets.UTF_8));
        	}        	
        });
        
        String cmd = params.getString("cmd");
        if("text2sql".equals(cmd)) {
    		// Text Query
        	JsonArray list = new JsonArray();
        	params.put("input_value", params.getString("text"));
        	String endpoint = params.getString("endpoint","text2sql");
        	ServiceResult r = langflowClient.call(endpoint,params);
        	if(r.getStatus().equals("200")) {
        		try {
		    		JsonArray outputs = r.getResult().getJsonArray("outputs").getJsonObject(0).getJsonArray("outputs");
		    		for(int i=0;i<outputs.size();i++) {
			    		String sql = outputs.getJsonObject(i).getJsonObject("results").getJsonObject("message").getString("text");
						list.add(sql);
		    		}
        		}
        		catch(Exception e) {
        			
        		}
			} 		
    		return RestResult.success(list.encode(), params.getString("sessionToken"));
		} 
        
        URL urlO = new URL(url);         
        if(urlO.getPath().isEmpty()) {
        	urlO = new URL(url+"/ignite");
        }
        HttpRequest request = HttpRequest.newBuilder()
        		        .uri(urlO.toURI())
        		        .timeout(Duration.ofMinutes(1))
        		        .header("Content-Type", "application/x-www-form-urlencoded")
        		        .POST(HttpRequest.BodyPublishers.ofString(fields.toString()))
        		        .build();        

        try {
            HttpResponse<String> res = httpClient.send(request,BodyHandlers.ofString());            
            RestResult result = parseResponse(res);            
            return result;
        }
        catch (Exception e) {
            throw e.getCause();
        }
        
    }
    
    /**
     * @param url Request URL.
     * @param params Request parameters.
     * @return Request result.
     * @throws IOException If failed to parse REST result.
     * @throws Throwable If failed to send request.
     */
    public HttpResponse<String> sendMetaServerRequest(String url, JsonObject params,String token) throws Throwable {
        
    	final StringBuilder fields = new StringBuilder();
    	if(params!=null) {
	        params.getMap().forEach((k, v) ->{ 
	        	if(v!=null) {
	        		if(fields.length()>0) {
	        			fields.append("&");
	        		}        		     		
	        		fields.append(k);
	        		fields.append("=");
	        		fields.append(URLEncoder.encode(String.valueOf(v),StandardCharsets.UTF_8));
	        	}        	
	        });
    	}
        if(fields.length()>0) {
        	url = url+"?"+fields.toString();
        }
        URL urlO = new URL(url);         
        
        HttpRequest request = HttpRequest.newBuilder()
        		        .uri(urlO.toURI())        		      
        		        .timeout(Duration.ofMinutes(1))
        		        .header("Content-Type", "application/json")
        		        .header("Authorization", "Token "+token)
        		        .GET()
        		        .build();        

        try {
            HttpResponse<String> res = httpClient.send(request,BodyHandlers.ofString()); 
            return res;
        }
        catch (Exception e) {
            throw e.getCause();
        }
        
    }
    
    /**
     * 
     * @param serverUri
     * @param clusterId
     * @param token
     * @return json: id,valueType,tableComment,fields
     * @throws Throwable
     */
    public JsonObject getMetadata(String serverUri,String clusterId,String token) throws Throwable {
    	if (serverUri.startsWith("ws:/")) {
			serverUri = serverUri.replaceAll("ws:/", "http:/");
		}
		if (serverUri.startsWith("wss:/")) {
			serverUri = serverUri.replaceAll("ws:/", "https:/");
		}
    	String clusterConfigGetUrl = serverUri+"/api/v1/configuration/clusters/";
    	HttpResponse<String> modelsResp = sendMetaServerRequest(clusterConfigGetUrl+clusterId+"/models", null,token);
    	
    	JsonArray models = new JsonArray(modelsResp.body());
    	JsonObject result = new JsonObject();
    	for(int i=0;i<models.size();i++) {
    		String modelConfigGetUrl = serverUri+"/api/v1/configuration/domains/";
    		JsonObject model = models.getJsonObject(i);
    		
    		HttpResponse<String> modelResp = sendMetaServerRequest(modelConfigGetUrl+model.getString("id"), null,token);
    		JsonObject modelJson = new JsonObject(modelResp.body());    		
    		
    		result.put(modelJson.getString("valueType"), modelJson);
    	}
    	
    	return result;
    }
    
    public JsonObject getCachedMetadata(String serverUri,String clusterId,String token) throws Throwable {
    	String key = clusterId;
    	JsonObject result = (JsonObject)cacheMap.get(key);
    	if(result==null) {
    		result = getMetadata(serverUri,clusterId,token);
    		cacheMap.put(key,result);
    	}
    	return result;
    }
}
