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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import org.slf4j.LoggerFactory;

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
    
    /** */
    private final HttpClient httpClient;

    /**
     * @param sslCtxFactory Ssl context factory.
     */
    public RestExecutor(SslContextFactory sslCtxFactory) {
        httpClient = HttpClient.newBuilder()
        		.connectTimeout(Duration.ofSeconds(60))
        		.followRedirects(Redirect.NEVER)
        		.build();
       
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
    		String text = "SELECT * from //" + params.getString("text");
			list.add(text);                    		
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
            return parseResponse(res);
        }
        catch (Exception e) {
            throw e.getCause();
        }
        
    }
}
