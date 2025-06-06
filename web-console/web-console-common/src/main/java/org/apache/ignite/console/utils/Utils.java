

package org.apache.ignite.console.utils;

import java.io.IOException;
import java.io.Reader;
import java.util.AbstractMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;

import org.apache.ignite.internal.util.typedef.F;

/**
 * Utilities.
 */
public class Utils {
    /** */
    private static final ObjectMapper MAPPER = DatabindCodec.mapper();
    
    private static final Pattern escapeFileNamePattern = Pattern.compile("[\\\\\\/*\"\\[\\],\\.:;|=<>?]", Pattern.CASE_INSENSITIVE);

    /**
     * Private constructor for utility class.
     */
    private Utils() {
        // No-op.
    }
    
    public static String escapeFileName(String clusterName) {
    	clusterName = clusterName.replaceAll(escapeFileNamePattern.pattern(), "-");
    	clusterName = clusterName.replace(' ', '_');
    	return clusterName;
    }

    /**
     * @param v Value to serialize.
     * @return JSON value.
     * @throws IllegalStateException If serialization failed.
     */
    public static String toJson(Object v) {
        try {
        	if (v instanceof JsonObject) {
                return v.toString();
        	}
        	if (v instanceof JsonArray) {
                return v.toString();
        	}
            return MAPPER.writeValueAsString(v);
        }
        catch (Throwable e) {
            throw new IllegalStateException("Failed to serialize as JSON: " + v, e);
        }
    }
    
    /**
     * @param json JSON.
     * @return Map with parameters.
     * @throws IllegalStateException If deserialization failed.
     */
    public static JsonObject asJson(Object json) {
        try {
        	if(json instanceof String) {
        		return new JsonObject(json.toString());
        	}
        	if(json instanceof byte[]) {
        		return fromJson((byte[])json);
        	}
        	if(json instanceof Map) {
        		return new JsonObject((Map)json);
        	}
        	if (json instanceof JsonObject) {
                return (JsonObject)json;
        	}

        	return new JsonObject(MAPPER.convertValue(json, Map.class));
        }
        catch (Throwable e) {
            throw new IllegalStateException("Failed to deserialize object from JSON: " + json, e);
        }
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
        	return new JsonObject(json);            
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
        	return new JsonObject(Buffer.buffer(json)); 
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
