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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.Dispatcher;
import okhttp3.FormBody;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.ignite.console.demo.*;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.log4j.Logger;

import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_AUTH_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/**
 * API to translate REST requests to Ignite cluster.
 */
public class RestExecutor {
    /** */
    private static final Logger log = Logger.getLogger(RestExecutor.class);

    /** JSON object mapper. */
    private static final ObjectMapper mapper = new GridJettyObjectMapper();

    /** */
    private final OkHttpClient httpClient;

    /** Node URL. */
    private String nodeUrl;

    /**
     * Default constructor.
     */
    public RestExecutor(String nodeUrl) {
        this.nodeUrl = nodeUrl;

        Dispatcher dispatcher = new Dispatcher();
        
        dispatcher.setMaxRequests(Integer.MAX_VALUE);
        dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);

        httpClient = new OkHttpClient.Builder()
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .dispatcher(dispatcher)
            .build();
    }

    /**
     * Stop HTTP client.
     */
    public void stop() {
        if (httpClient != null) {
            httpClient.dispatcher().executorService().shutdown();

            httpClient.dispatcher().cancelAll();
        }
    }

    /** */
    private RestResult sendRequest(boolean demo, String path, Map<String, Object> params,
        String mtd, Map<String, Object> headers, String body) throws IOException {
        if (demo && AgentClusterDemo.getDemoUrl() == null) {
            try {
                AgentClusterDemo.tryStart().await();
            }
            catch (InterruptedException ignore) {
                throw new IllegalStateException("Failed to execute request because of embedded node for demo mode is not started yet.");
            }
        }

        String url = demo ? AgentClusterDemo.getDemoUrl() : nodeUrl;

        HttpUrl.Builder urlBuilder = HttpUrl.parse(url)
            .newBuilder();

        if (path != null)
            urlBuilder.addPathSegment(path);

        final Request.Builder reqBuilder = new Request.Builder();

        if (headers != null) {
            for (Map.Entry<String, Object> entry : headers.entrySet())
                if (entry.getValue() != null)
                    reqBuilder.addHeader(entry.getKey(), entry.getValue().toString());
        }

        if ("GET".equalsIgnoreCase(mtd)) {
            if (params != null) {
                for (Map.Entry<String, Object> entry : params.entrySet()) {
                    if (entry.getValue() != null)
                        urlBuilder.addQueryParameter(entry.getKey(), entry.getValue().toString());
                }
            }
        }
        else if ("POST".equalsIgnoreCase(mtd)) {
            if (body != null) {
                MediaType contentType = MediaType.parse("text/plain");

                reqBuilder.post(RequestBody.create(contentType, body));
            }
            else {
                FormBody.Builder formBody = new FormBody.Builder();

                if (params != null) {
                    for (Map.Entry<String, Object> entry : params.entrySet()) {
                        if (entry.getValue() != null)
                            formBody.add(entry.getKey(), entry.getValue().toString());
                    }
                }

                reqBuilder.post(formBody.build());
            }
        }
        else
            throw new IllegalArgumentException("Unknown HTTP-method: " + mtd);

        reqBuilder.url(urlBuilder.build());

        try (Response resp = httpClient.newCall(reqBuilder.build()).execute()) {
            String content = resp.body().string();

            if (resp.isSuccessful()) {
                JsonNode node = mapper.readTree(content);

                int status = node.get("successStatus").asInt();

                switch (status) {
                    case STATUS_SUCCESS:
                        return RestResult.success(node.get("response").toString());

                    default:
                        return RestResult.fail(status, node.get("error").asText());
                }
            }

            if (resp.code() == 401)
                return RestResult.fail(STATUS_AUTH_FAILED, "Failed to authenticate in grid. Please check agent\'s login and password or node port.");

            return RestResult.fail(STATUS_FAILED, "Failed connect to node and execute REST command.");
        }
        catch (ConnectException ignore) {
            throw new ConnectException("Failed connect to node and execute REST command [url=" + urlBuilder + "]");
        }
    }

    /**
     * @param demo Is demo node request.
     * @param path Path segment.
     * @param params Params.
     * @param mtd Method.
     * @param headers Headers.
     * @param body Body.
     */
    public RestResult execute(boolean demo, String path, Map<String, Object> params,
        String mtd, Map<String, Object> headers, String body) {
        log.debug("Start execute REST command [method=" + mtd + ", uri=/" + (path == null ? "" : path) +
                ", parameters=" + params + "]");

        try {
            return sendRequest(demo, path, params, mtd, headers, body);
        }
        catch (Exception e) {
            log.info("Failed to execute REST command [method=" + mtd + ", uri=/" + (path == null ? "" : path) +
                ", parameters=" + params + "]", e);

            return RestResult.fail(404, e.getMessage());
        }
    }

    /**
     * @param demo Is demo node request.
     */
    public RestResult topology(boolean demo, boolean full) throws IOException {
        Map<String, Object> params = new HashMap<>(3);

        params.put("cmd", "top");
        params.put("attr", true);
        params.put("mtr", full);

        return sendRequest(demo, "ignite", params, "GET", null, null);
    }
}
