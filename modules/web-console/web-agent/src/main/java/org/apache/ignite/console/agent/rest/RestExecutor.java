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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.io.StringWriter;
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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.demo.AgentClusterDemo;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.slf4j.LoggerFactory;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_AUTH_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_FAILED;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;

/**
 * API to translate REST requests to Ignite cluster.
 */
public class RestExecutor {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(RestExecutor.class));

    /** JSON object mapper. */
    private static final ObjectMapper MAPPER = new GridJettyObjectMapper();

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
        Map<String, Object> headers, String body) throws IOException {
        if (demo && AgentClusterDemo.getDemoUrl() == null) {
            try {
                AgentClusterDemo.tryStart().await();
            }
            catch (InterruptedException ignore) {
                throw new IllegalStateException("Failed to send request because of embedded node for demo mode is not started yet.");
            }
        }

        String url = demo ? AgentClusterDemo.getDemoUrl() : nodeUrl;

        HttpUrl httpUrl = HttpUrl.parse(url);

        if (httpUrl == null)
            throw new IllegalStateException("Failed to send request because of node URL is invalid: " + url);

        HttpUrl.Builder urlBuilder = httpUrl.newBuilder();

        if (path != null)
            urlBuilder.addPathSegment(path);

        final Request.Builder reqBuilder = new Request.Builder();

        if (headers != null) {
            for (Map.Entry<String, Object> entry : headers.entrySet())
                if (entry.getValue() != null)
                    reqBuilder.addHeader(entry.getKey(), entry.getValue().toString());
        }

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
        
        reqBuilder.url(urlBuilder.build());

        try (Response resp = httpClient.newCall(reqBuilder.build()).execute()) {
            if (resp.isSuccessful()) {
                RestResponseHolder res = MAPPER.readValue(resp.body().byteStream(), RestResponseHolder.class);

                int status = res.getSuccessStatus();

                switch (status) {
                    case STATUS_SUCCESS:
                        return RestResult.success(res.getResponse());

                    default:
                        return RestResult.fail(status, res.getError());
                }
            }

            if (resp.code() == 401)
                return RestResult.fail(STATUS_AUTH_FAILED, "Failed to authenticate in cluster. " +
                    "Please check agent\'s login and password or node port.");

            if (resp.code() == 404)
                return RestResult.fail(STATUS_FAILED, "Failed connect to cluster.");

            return RestResult.fail(STATUS_FAILED, "Failed to execute REST command: " + resp.message());
        }
        catch (ConnectException ignored) {
            LT.warn(log, "Failed connect to cluster. " +
                "Please ensure that nodes have [ignite-rest-http] module in classpath " +
                "(was copied from libs/optional to libs folder).");

            throw new ConnectException("Failed connect to cluster [url=" + urlBuilder + ", parameters=" + params + "]");
        }
    }

    /**
     * @param demo Is demo node request.
     * @param path Path segment.
     * @param params Params.
     * @param headers Headers.
     * @param body Body.
     */
    public RestResult execute(boolean demo, String path, Map<String, Object> params,
        Map<String, Object> headers, String body) {
        if (log.isDebugEnabled())
            log.debug("Start execute REST command [uri=/" + (path == null ? "" : path) +
                ", parameters=" + params + "]");

        try {
            return sendRequest(demo, path, params, headers, body);
        }
        catch (Exception e) {
            U.error(log, "Failed to execute REST command [uri=/" + (path == null ? "" : path) +
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

        return sendRequest(demo, "ignite", params, null, null);
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
        private String sesTokStr;

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
            return sesTokStr;
        }

        /**
         * @param sesTokStr String representation of session token.
         */
        public void setSessionToken(String sesTokStr) {
            this.sesTokStr = sesTokStr;
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
