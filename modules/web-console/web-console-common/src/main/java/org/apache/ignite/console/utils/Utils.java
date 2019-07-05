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

package org.apache.ignite.console.utils;

import java.io.IOException;
import java.io.Reader;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.internal.processors.rest.protocols.http.jetty.GridJettyObjectMapper;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Utilities.
 */
public class Utils {
    /** */
    private static final GridJettyObjectMapper MAPPER = new GridJettyObjectMapper();

    /**
     * Private constructor for utility class.
     */
    private Utils() {
        // No-op.
    }

    /**
     * @param v Value to serialize.
     * @return JSON value.
     * @throws IllegalStateException If serialization failed.
     */
    public static String toJson(Object v) {
        try {
            return MAPPER.writeValueAsString(v);
        }
        catch (Throwable e) {
            throw new IllegalStateException("Failed to serialize as JSON: " + v, e);
        }
    }

    /**
     * Cast object to JSON.
     *
     * @param v Object.
     * @return JSON object.
     */
    @SuppressWarnings("unchecked")
    public static JsonObject asJson(Object v) {
        if (v instanceof  JsonObject)
            return (JsonObject)v;

        if (v instanceof Map)
            return new JsonObject((Map)v);

        throw new ClassCastException("Not a JSON");
    }

    /**
     * @param json JSON.
     * @param cls Object class.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     */
    public static <T> T fromJson(String json, Class<T> cls) throws IOException {
        return MAPPER.readValue(json, cls);
    }

    /**
     * @param json JSON.
     * @param cls Object class.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     */
    public static <T> T fromJson(byte[] json, Class<T> cls) throws IOException {
        return MAPPER.readValue(json, cls);
    }

    /**
     * @param json JSON.
     * @param typeRef Type descriptor.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     */
    public static <T> T fromJson(byte[] json, TypeReference<T> typeRef) throws IOException {
        return MAPPER.readValue(json, typeRef);
    }

    /**
     * @param src source of JSON.
     * @param cls Object class.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     */
    public static <T> T fromJson(Reader src, Class<T> cls) throws IOException {
        return MAPPER.readValue(src, cls);
    }

    /**
     * @param json JSON.
     * @param typeRef Type descriptor.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     */
    public static <T> T fromJson(String json, TypeReference<T> typeRef) throws IOException {
        return MAPPER.readValue(json, typeRef);
    }

    /**
     * @param json JSON.
     * @return Map with parameters.
     * @throws IllegalStateException If deserialization failed.
     */
    public static JsonObject fromJson(String json) {
        try {
            return MAPPER.readValue(json, JsonObject.class);
        }
        catch (Throwable e) {
            throw new IllegalStateException("Failed to deserialize object from JSON: " + json, e);
        }
    }

    /**
     * @param json JSON.
     * @return Map with parameters.
     * @throws IllegalStateException If deserialization failed.
     */
    public static JsonObject fromJson(byte[] json) {
        try {
            return MAPPER.readValue(json, JsonObject.class);
        }
        catch (Throwable e) {
            throw new IllegalStateException("Failed to deserialize object from JSON", e);
        }
    }

    /**
     * Helper method to get attribute.
     *
     * @param attrs Map with attributes.
     * @param name Attribute name.
     * @return Attribute value.
     */
    public static <T> T attribute(Map<String, Object> attrs, String name) {
        return (T)attrs.get(name);
    }

    /**
     * @param cause Error.
     * @return Error message or exception class name.
     */
    public static String errorMessage(Throwable cause) {
        String msg = cause.getMessage();

        return F.isEmpty(msg) ? cause.getClass().getName() : msg;
    }

    /**
     * @param prefix Message prefix.
     * @param e Exception.
     */
    public static String extractErrorMessage(String prefix, Throwable e) {
        String causeMsg = F.isEmpty(e.getMessage()) ? e.getClass().getName() : e.getMessage();

        return prefix + ": " + causeMsg;
    }

    /**
     * Simple entry generator.
     *
     * @param key Key.
     * @param val Value.
     */
    public static <K, V> Map.Entry<K, V> entry(K key, V val) {
        return new AbstractMap.SimpleEntry<>(key, val);
    }

    /**
     * Collector.
     */
    public static <K, U> Collector<Map.Entry<K, U>, ?, Map<K, U>> entriesToMap() {
        return Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue);
    }
}
