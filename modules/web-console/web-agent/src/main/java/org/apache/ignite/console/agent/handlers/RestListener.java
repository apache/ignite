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

package org.apache.ignite.console.agent.handlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.codec.Charsets;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.demo.AgentClusterDemo;
import org.apache.log4j.Logger;

import static org.apache.ignite.console.agent.AgentConfiguration.DFLT_NODE_PORT;

/**
 * API to translate REST requests to Ignite cluster.
 */
public class RestListener extends AbstractListener {
    /** */
    private static final Logger log = Logger.getLogger(RestListener.class.getName());

    /** */
    private final AgentConfiguration cfg;

    /** */
    private CloseableHttpClient httpClient;

    /**
     * @param cfg Config.
     */
    public RestListener(AgentConfiguration cfg) {
        super();

        this.cfg = cfg;

        // Create a connection manager with custom configuration.
        PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager();

        connMgr.setDefaultMaxPerRoute(Integer.MAX_VALUE);
        connMgr.setMaxTotal(Integer.MAX_VALUE);

        httpClient = HttpClientBuilder.create().setConnectionManager(connMgr).build();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        super.stop();

        if (httpClient != null) {
            try {
                httpClient.close();
            }
            catch (IOException ignore) {
                // No-op.
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected ExecutorService newThreadPool() {
        return Executors.newCachedThreadPool();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Object execute(Map<String, Object> args) throws Exception {
        if (log.isDebugEnabled())
            log.debug("Start parse REST command args: " + args);

        String uri = null;

        if (args.containsKey("uri"))
            uri = args.get("uri").toString();

        Map<String, Object> params = null;

        if (args.containsKey("params"))
            params = (Map<String, Object>)args.get("params");

        if (!args.containsKey("demo"))
            throw new IllegalArgumentException("Missing demo flag in arguments: " + args);

        boolean demo = (boolean)args.get("demo");

        if (!args.containsKey("method"))
            throw new IllegalArgumentException("Missing method in arguments: " + args);

        String mtd = args.get("method").toString();

        Map<String, Object> headers = null;

        if (args.containsKey("headers"))
            headers = (Map<String, Object>)args.get("headers");

        String body = null;

        if (args.containsKey("body"))
            body = args.get("body").toString();

        return executeRest(uri, params, demo, mtd, headers, body);
    }

    /**
     * @param uri Url.
     * @param params Params.
     * @param demo Use demo node.
     * @param mtd Method.
     * @param headers Headers.
     * @param body Body.
     */
    protected RestResult executeRest(String uri, Map<String, Object> params, boolean demo,
        String mtd, Map<String, Object> headers, String body) throws IOException, URISyntaxException {
        if (log.isDebugEnabled())
            log.debug("Start execute REST command [method=" + mtd + ", uri=/" + (uri == null ? "" : uri) +
                ", parameters=" + params + "]");

        final URIBuilder builder;

        if (demo) {
            // try start demo if needed.
            AgentClusterDemo.testDrive(cfg);

            // null if demo node not started yet.
            if (cfg.demoNodeUri() == null)
                return RestResult.fail("Demo node is not started yet.", 404);

            builder = new URIBuilder(cfg.demoNodeUri());
        }
        else
            builder = new URIBuilder(cfg.nodeUri());

        if (builder.getPort() == -1)
            builder.setPort(DFLT_NODE_PORT);

        if (uri != null) {
            if (!uri.startsWith("/") && !cfg.nodeUri().endsWith("/"))
                uri = '/' + uri;

            builder.setPath(uri);
        }

        if (params != null) {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                if (entry.getValue() != null)
                    builder.addParameter(entry.getKey(), entry.getValue().toString());
            }
        }

        HttpRequestBase httpReq = null;

        try {
            if ("GET".equalsIgnoreCase(mtd))
                httpReq = new HttpGet(builder.build());
            else if ("POST".equalsIgnoreCase(mtd)) {
                HttpPost post;

                if (body == null) {
                    List<NameValuePair> nvps = builder.getQueryParams();

                    builder.clearParameters();

                    post = new HttpPost(builder.build());

                    if (!nvps.isEmpty())
                        post.setEntity(new UrlEncodedFormEntity(nvps));
                }
                else {
                    post = new HttpPost(builder.build());

                    post.setEntity(new StringEntity(body));
                }

                httpReq = post;
            }
            else
                throw new IOException("Unknown HTTP-method: " + mtd);

            if (headers != null) {
                for (Map.Entry<String, Object> entry : headers.entrySet())
                    httpReq.addHeader(entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
            }

            try (CloseableHttpResponse resp = httpClient.execute(httpReq)) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();

                resp.getEntity().writeTo(out);

                Charset charset = Charsets.UTF_8;

                Header encodingHdr = resp.getEntity().getContentEncoding();

                if (encodingHdr != null) {
                    String encoding = encodingHdr.getValue();

                    charset = Charsets.toCharset(encoding);
                }

                return RestResult.success(resp.getStatusLine().getStatusCode(), new String(out.toByteArray(), charset));
            }
            catch (ConnectException e) {
                log.info("Failed connect to node and execute REST command [uri=" + builder.build() + "]");

                return RestResult.fail("Failed connect to node and execute REST command.", 404);
            }
        }
        finally {
            if (httpReq != null)
                httpReq.reset();
        }
    }

    /**
     * Request result.
     */
    public static class RestResult {
        /** The field contains description of error if server could not handle the request. */
        public final String error;

        /** REST http code. */
        public final int code;

        /** The field contains result of command. */
        public final String data;

        /**
         * @param error The field contains description of error if server could not handle the request.
         * @param resCode REST http code.
         * @param res The field contains result of command.
         */
        private RestResult(String error, int resCode, String res) {
            this.error = error;
            this.code = resCode;
            this.data = res;
        }

        /**
         * @param error The field contains description of error if server could not handle the request.
         * @param restCode REST http code.
         * @return Request result.
         */
        public static RestResult fail(String error, int restCode) {
            return new RestResult(error, restCode, null);
        }

        /**
         * @param code REST http code.
         * @param data The field contains result of command.
         * @return Request result.
         */
        public static RestResult success(int code, String data) {
            return new RestResult(null, code, data);
        }
    }
}
