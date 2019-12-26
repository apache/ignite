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

package org.apache.ignite.springsession.annotation;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.ignite.springsession.IgniteSession;
import org.springframework.core.ConfigurableObjectInputStream;
import org.springframework.core.NestedIOException;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.serializer.support.SerializationFailedException;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.session.SessionRepository;
import org.springframework.stereotype.Component;

/**
 * Session repository backed with Apache Ignite via REST.
 */
@Component
public class IgniteRestSessionRepository implements SessionRepository<IgniteSession> {
    /** Default Ignite URL. */
    public final static String DFLT_URL = "http://localhost:8080";

    /** Default session cache name. */
    public static final String DFLT_SESSION_STORAGE_NAME = "spring.session.cache";

    /** Cmd. */
    private final static String CMD = "cmd";

    /** Get command. */
    private final static String GET = "get";

    /** Put command. */
    private final static String PUT = "put";

    /** Remove command. */
    private final static String DELETE = "rmv";

    /** Get or create cache command. */
    private final static String CREATE_CACHE = "getOrCreate";

    /** Cache name command. */
    private static final String CACHE_NAME = "cacheName";

    /** Key. */
    private static final String KEY = "key";

    /** Value. */
    private static final String VALUE = "val";

    /** Cache template name. */
    private static final String CACHE_TEMPLATE = "templateName";

    /** Replicated cache mode. */
    private static final String REPLICATED = "REPLICATED";

    /** Partitioned cache mode. */
    private static final String PARTITIONED = "PARTITIONED";

    /** URL. */
    private String url = DFLT_URL;

    /** Session cache name. */
    private String sessionCacheName;

    /** Default max inactive interval for session. */
    private Integer defaultMaxInactiveInterval;

    /** Mapper. */
    private ObjectMapper mapper;

    /** Conversion service to transform session to byte array. */
    private ConversionService conversionService;

    /** Constructor. */
    public IgniteRestSessionRepository() {

    }

    /**
     * Constructor.
     *
     * @param url URL.
     */
    public IgniteRestSessionRepository(String url) {
        this.url = url;
    }

    /**
     * Creates default conversion service.
     *
     * @return Conversion service.
     */
    private static GenericConversionService createDefaultConversionService() {
        GenericConversionService converter = new GenericConversionService();
        converter.addConverter(IgniteSession.class, byte[].class,
                new SerializingConverter());
        converter.addConverter(byte[].class, IgniteSession.class,
                new IgniteSessionDeserializingConverter());
        return converter;
    }

    /**
     * Serializes attribute value.
     *
     * @param attributeValue Attribute value.
     * @return Serialized value.
     */
    private String serialize(IgniteSession attributeValue) {
        byte[] value = (byte[]) this.conversionService.convert(attributeValue,
                TypeDescriptor.valueOf(IgniteSession.class),
                TypeDescriptor.valueOf(byte[].class));
        return Hex.encodeHexString(value);
    }

    /**
     * Deserializes string value into {@link IgniteSession}.
     *
     * @param value Session string.
     * @return Session instance.
     * @throws DecoderException If cannot be decoded.
     */
    private IgniteSession deserialize(String value) throws DecoderException {
        if ("null".equals(value))
            return null;
        return (IgniteSession) this.conversionService.convert(Hex.decodeHex(value.toCharArray()),
                TypeDescriptor.valueOf(byte[].class),
                TypeDescriptor.valueOf(IgniteSession.class));
    }

    /**
     * Initialize repository, create cache if it doesn't exist.
     */
    @PostConstruct
    public void init() {
        this.mapper = new ObjectMapper();
        this.mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        this.mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        this.conversionService = createDefaultConversionService();

        executeSessionCacheCommand(CREATE_CACHE, CACHE_TEMPLATE, REPLICATED);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSession createSession() {
        IgniteSession session = new IgniteSession();

        if (this.defaultMaxInactiveInterval != null)
            session.setMaxInactiveIntervalInSeconds(this.defaultMaxInactiveInterval);

        return session;
    }

    /** {@inheritDoc} */
    @Override
    public void save(IgniteSession session) {
        executeSessionCacheCommand(PUT, KEY, session.getId(), VALUE, serialize(session));
    }

    /** {@inheritDoc} */
    @Override
    public void delete(String id) {
        executeSessionCacheCommand(DELETE, KEY, id);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSession getSession(final String id) {
        ResponseHandler<IgniteSession> hnd = new ResponseHandler<IgniteSession>() {
            /** {@inheritDoc} */
            @Override
            public IgniteSession handleResponse(HttpResponse res) throws IOException {
                assert res.getStatusLine().getStatusCode() == 200;

                InputStream is = null;
                try {
                    is = res.getEntity().getContent();
                    JsonNode node = mapper.readTree(is).get("response");
                    IgniteSession session = deserialize(node.asText());

                    if (session == null)
                        return null;

                    if (session.isExpired()) {
                        delete(id);
                        return null;
                    }

                    return session;
                } catch (DecoderException e) {
                    e.printStackTrace();
                } finally {
                    if (is != null)
                        is.close();
                }

                return null;
            }
        };

        return executeSessionCacheCommand(hnd, GET, KEY, id);
    }

    /**
     * Creates http client.
     *
     * @return Http client.
     */
    private CloseableHttpClient createHttpClient() {
        return HttpClients.custom().build();
    }

    /**
     * Executes cache command for current session.
     *
     * @param command Command to execute.
     * @param args Arguments.
     */
    private void executeSessionCacheCommand(String command, String... args) {
        executeSessionCacheCommand(null, command, args);
    }

    /**
     * Executes cache command for current session.
     *
     * @param hnd Response handler.
     * @param command Command to execute.
     * @param args Arguments.
     * @param <T> Response type.
     * @return Response instance.
     */
    private <T> T executeSessionCacheCommand(ResponseHandler<? extends T> hnd, String command, String... args) {
        CloseableHttpClient client = createHttpClient();
        try {
            HttpUriRequest req = buildCacheCommandRequest(this.url, command, this.sessionCacheName, args);

            if (hnd != null)
                return client.execute(req, hnd);
            else {
                CloseableHttpResponse res = client.execute(req);

                assert res.getStatusLine().getStatusCode() == 200;

                res.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    /**
     * Executes cache command for current session.
     *
     * @param urlAddr Ignite rest endpoint.
     * @param command Command to execute.
     * @param cacheName Cache name.
     * @param params Parameters.
     * @return Constructed request.
     * @throws Exception If fails.
     */
    private HttpUriRequest buildCacheCommandRequest(String urlAddr, String command, String cacheName,
                                                    String... params) throws Exception {
        if (params.length % 2 != 0)
            throw new IllegalArgumentException("Number of parameters should be even");

        List<NameValuePair> paramList = new ArrayList<NameValuePair>(params.length / 2);

        for (int i = 0; i < params.length; i += 2) {
            String key = params[i];
            String val = params[i + 1];

            paramList.add(new BasicNameValuePair(key, val));
        }

        URL url = new URL(urlAddr + "/ignite" +
                "?" + CMD + '=' + command +
                "&" + CACHE_NAME + '=' + cacheName);
        HttpPost req = new HttpPost(url.toURI());
        req.setEntity(new UrlEncodedFormEntity(paramList));

        return req;
    }

    /**
     * Sets Ignite url.
     *
     * @param url
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Sets session cache name.
     *
     * @param sessionCacheName
     */
    public void setSessionCacheName(String sessionCacheName) {
        this.sessionCacheName = sessionCacheName;
    }

    /**
     * Sets default max inactive time interval for session to expire.
     *
     * @param dfltMaxInactiveInterval
     */
    public void setDefaultMaxInactiveInterval(Integer dfltMaxInactiveInterval) {
        defaultMaxInactiveInterval = dfltMaxInactiveInterval;
    }

    /**
     * Ignite session deserializer.
     */
    private static class IgniteSessionDeserializingConverter implements Converter<byte[], IgniteSession> {

        /** {@inheritDoc} */
        @Override public IgniteSession convert(byte[] bytes) {
            ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);

            try {
                ConfigurableObjectInputStream objectInputStream = new ConfigurableObjectInputStream(byteStream, null);
                return (IgniteSession)objectInputStream.readObject();
            }
            catch (Throwable e) {
                throw new SerializationFailedException("Failed to deserialize payload. Is the byte array a result of corresponding serialization for " + IgniteSessionDeserializingConverter.class.getSimpleName() + "?", e);
            }
        }
    }

    /**
     * Ignite session serializer.
     */
    private class IgniteSessionSerializingConverter implements Converter<IgniteSession, byte[]> {

        /** {@inheritDoc} */
        @Override public byte[] convert(IgniteSession bytes) {
            return null;
        }
    }

}
