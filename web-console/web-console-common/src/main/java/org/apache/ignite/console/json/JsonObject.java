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

package org.apache.ignite.console.json;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A helper class to build a JSON object in Java.
 */
public class JsonObject extends LinkedHashMap<String, Object> {
    /**
     * Default constructor.
     */
    public JsonObject() {
        // No-op.
    }

    /**
     * Wrapping constructor.
     *
     * @param map Map with values.
     */
    public JsonObject(Map<String, Object> map) {
        super(map);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @return {@code this} for chaining.
     */
    public JsonObject add(String key, Object val) {
        put(key, val);

        return this;
    }

    /**
     * Get the boolean value for specified key.
     *
     * @param key Key.
     * @return Value or {@code null} if no value for that key.
     * @throws ClassCastException If the value is not a {@link Boolean}.
     */
    public Boolean getBoolean(String key) {
        return (Boolean)get(key);
    }

    /**
     * Get the boolean value for specified key.
     *
     * @param key Key.
     * @param dflt Default value, if map does not contain key.
     * @return Value or {@code dflt} if no value for that key.
     * @throws ClassCastException If the value is not a {@link Boolean}.
     */
    public Boolean getBoolean(String key, boolean dflt) {
        Boolean val = (Boolean)get(key);

        return val != null || containsKey(key) ? val : dflt;
    }

    /**
     * Get the string value for specified key.
     *
     * @param key Key.
     * @return Value or {@code null} if no value for that key.
     * @throws ClassCastException If the value is not a {@link String}.
     */
    public String getString(String key) {
        return (String)get(key);
    }

    /**
     * Get the string value for specified key.
     *
     * @param key Key.
     * @param dflt Default value, if map does not contain key.
     * @return Value or {@code dflt} if no value for that key.
     * @throws ClassCastException If the value is not a {@link String}.
     */
    public String getString(String key, String dflt) {
        String val = (String)get(key);

        return val != null || containsKey(key) ? val : dflt;
    }

    /**
     * Get the integer value for specified key.
     *
     * @param key Key.
     * @return Value or {@code null} if no value for that key.
     * @throws ClassCastException If the value is not a {@link Integer}.
     */
    public Integer getInteger(String key) {
        return (Integer)get(key);
    }

    /**
     * Get the integer value for specified key.
     *
     * @param key Key.
     * @param dflt Default value, if map does not contain value for key.
     * @return Value or {@code dflt} if no value for that key.
     * @throws ClassCastException If the value is not a {@link Integer}.
     */
    public Integer getInteger(String key, Integer dflt) {
        Integer val = (Integer)get(key);

        return val != null ? val : dflt;
    }

    /**
     * Get the long value for specified key.
     *
     * @param key Key.
     * @param dflt Default value, if map does not contain value for key.
     * @return Value or {@code dflt} if no value for that key.
     * @throws ClassCastException If the value is not a {@link Long}.
     */
    public Long getLong(String key, Long dflt) {
        Long val = (Long)get(key);

        return val != null ? val : dflt;
    }

    /**
     * Get the UUID value for specified key.
     *
     * @param key Key.
     * @return Value or {@code null} if no value for that key.
     * @throws ClassCastException If the value is not a {@link UUID}.
     */
    public UUID getUuid(String key) {
        Object val = get(key);

        if (val == null)
            return null;

        if (val instanceof UUID)
            return (UUID)val;

        if (val instanceof String)
            return UUID.fromString(val.toString());

        throw new ClassCastException("Failed to get UUID from: " + val);
    }

    /**
     * Get JSON object.
     *
     * @param key Key.
     * @return Value or {@code null} if no value for that key.
     * @throws ClassCastException If the value is not a JSON object.
     */
    @SuppressWarnings("unchecked")
    public JsonObject getJsonObject(String key) {
        Object val = get(key);

        if (val == null)
            return null;

        if (val instanceof JsonObject)
            return (JsonObject)val;

        if (val instanceof Map)
            return new JsonObject((Map)val);

        throw new ClassCastException("Failed to get JSON from: " + val);
    }

    /**
     * Get JSON array.
     * @param key Key.
     * @return Value or {@code null} if no value for that key.
     * @throws ClassCastException If the value is not a JSON array.
     */
    public JsonArray getJsonArray(String key) {
        Object val = get(key);

        if (val == null)
            return null;

        if (val instanceof JsonArray)
            return (JsonArray)val;

        if (val instanceof Collection)
            return new JsonArray((Collection)val);

        throw new ClassCastException("Failed to get JSON from: " + val);
    }

    /**
     * Merge in another JSON object.
     *
     * @param other JSON object to merge.
     * @return {@code this} for chaining.
     */
    public JsonObject mergeIn(JsonObject other) {
        putAll(other);

        return this;
    }
}
