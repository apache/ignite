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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.LoggerFactory;

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
        httpClient = new HttpClient(sslCtxFactory);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
            httpClient.stop();
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
    private RestResult parseResponse(Response res, InputStream in) throws IOException {
        int code = res.getStatus();

        if (code == HTTP_OK)
            return fromJson(new InputStreamReader(in), RestResult.class);

        if (code == HTTP_UNAUTHORIZED) {
            return RestResult.fail(STATUS_AUTH_FAILED, "Failed to authenticate in cluster. " +
                "Please check agent\'s login and password or node port.");
        }

        if (code == HTTP_NOT_FOUND)
            return RestResult.fail(STATUS_FAILED, "Failed connect to cluster.");

        return RestResult.fail(STATUS_FAILED, "Failed to execute REST command [code=" +
            code + ", msg=" + res.getReason() + "]");
    }

    /**
     * @param url Request URL.
     * @param params Request parameters.
     * @return Request result.
     * @throws IOException If failed to parse REST result.
     * @throws Throwable If failed to send request.
     */
    public RestResult sendRequest(String url, JsonObject params) throws Throwable {
        if (!httpClient.isRunning())
            httpClient.start();

        Fields fields = new Fields();

        params.forEach((k, v) ->{ if(v!=null) fields.add(k, String.valueOf(v)); });

        InputStreamResponseListener lsnr = new InputStreamResponseListener();
        
        URL urlO = new URL(url); 
        String path = "/ignite";
        if(!urlO.getPath().isEmpty()) {
        	path = "/"+URLEncoder.encode(urlO.getPath().substring(1),"UTF-8");
        }
        httpClient.newRequest(url)
            .path(path)
            .method(HttpMethod.POST)
            .content(new FormContentProvider(fields))
            .send(lsnr);

        try {
            Response res = lsnr.get(6L, TimeUnit.SECONDS);

            return parseResponse(res, lsnr.getInputStream());
        }
        catch (ExecutionException e) {
            throw e.getCause();
        }
    }
}
