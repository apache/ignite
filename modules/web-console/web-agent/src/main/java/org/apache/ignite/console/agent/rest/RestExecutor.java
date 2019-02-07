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
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
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
 * API to translate REST requests to Ignite cluster.
 */
public class RestExecutor implements AutoCloseable {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(RestExecutor.class));

    /** JSON object mapper. */
    private static final ObjectMapper MAPPER = new GridJettyObjectMapper();

    /** */
    private final OkHttpClient httpClient;

    /** Index of alive node URI. */
    private final Map<List<String>, Integer> startIdxs = U.newHashMap(2);

    /**
     * Constructor.
     *
     * @param trustAll {@code true} If we trust to self-signed sertificates.
     * @param keyStorePath Optional path to key store file.
     * @param keyStorePwd Optional password for key store.
     * @param trustStorePath Optional path to trust store file.
     * @param trustStorePwd Optional password for trust store.
     * @param cipherSuites Optional cipher suites.
     * @throws GeneralSecurityException If failed to initialize SSL.
     * @throws IOException If failed to load content of key stores.
     */
    public RestExecutor(
        boolean trustAll,
        String keyStorePath,
        String keyStorePwd,
        String trustStorePath,
        String trustStorePwd,
        List<String> cipherSuites
    ) throws GeneralSecurityException, IOException {
        Dispatcher dispatcher = new Dispatcher();

        dispatcher.setMaxRequests(Integer.MAX_VALUE);
        dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);

        OkHttpClient.Builder builder = new OkHttpClient.Builder()
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .dispatcher(dispatcher);

        X509TrustManager trustMgr = trustManager(trustAll, trustStorePath, trustStorePwd);

        if (trustAll)
            builder.hostnameVerifier((hostname, session) -> true);

        SSLSocketFactory sslSocketFactory = sslSocketFactory(
            keyStorePath, keyStorePwd,
            trustMgr,
            cipherSuites
        );

        if (sslSocketFactory != null) {
            if (trustMgr != null)
                builder.sslSocketFactory(sslSocketFactory, trustMgr);
            else
                builder.sslSocketFactory(sslSocketFactory);

            if (!F.isEmpty(cipherSuites))
                builder.connectionSpecs(sslConnectionSpec(cipherSuites));
        }

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
    private RestResult parseResponse(Response res) throws IOException {
        if (res.isSuccessful()) {
            RestResponseHolder holder = MAPPER.readValue(res.body().byteStream(), RestResponseHolder.class);

            int status = holder.getSuccessStatus();

            if (status == STATUS_SUCCESS)
                return RestResult.success(holder.getResponse(), holder.getSessionToken());

            return RestResult.fail(status, holder.getError());
        }

        if (res.code() == 401)
            return RestResult.fail(STATUS_AUTH_FAILED, "Failed to authenticate in cluster. " +
                "Please check agent\'s login and password or node port.");

        if (res.code() == 404)
            return RestResult.fail(STATUS_FAILED, "Failed connect to cluster.");

        return RestResult.fail(STATUS_FAILED, "Failed to execute REST command: " + res);
    }

    /** */
    private RestResult sendRequest(String url, Map<String, Object> params, Map<String, Object> headers) throws IOException {
        HttpUrl httpUrl = HttpUrl
            .parse(url)
            .newBuilder()
            .addPathSegment("ignite")
            .build();

        final Request.Builder reqBuilder = new Request.Builder();

        if (headers != null) {
            for (Map.Entry<String, Object> entry : headers.entrySet())
                if (entry.getValue() != null)
                    reqBuilder.addHeader(entry.getKey(), entry.getValue().toString());
        }

        FormBody.Builder bodyParams = new FormBody.Builder();

        if (params != null) {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                if (entry.getValue() != null)
                    bodyParams.add(entry.getKey(), entry.getValue().toString());
            }
        }

        reqBuilder.url(httpUrl).post(bodyParams.build());

        try (Response resp = httpClient.newCall(reqBuilder.build()).execute()) {
            return parseResponse(resp);
        }
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
    public RestResult sendRequest(
        List<String> nodeURIs,
        Map<String, Object> params,
        Map<String, Object> headers
    ) throws IOException {
        Integer startIdx = startIdxs.getOrDefault(nodeURIs, 0);

        int urlsCnt = nodeURIs.size();

        for (int i = 0;  i < urlsCnt; i++) {
            Integer currIdx = (startIdx + i) % urlsCnt;

            String nodeUrl = nodeURIs.get(currIdx);

            try {
                RestResult res = sendRequest(nodeUrl, params, headers);

                // If first attempt failed then throttling should be cleared.
                if (i > 0)
                    LT.clear();

                LT.info(log, "Connected to cluster [url=" + nodeUrl + "]");

                startIdxs.put(nodeURIs, currIdx);

                return res;
            }
            catch (ConnectException ignored) {
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
