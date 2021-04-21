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

package org.apache.ignite.configuration.internal.rest;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.join;

public class JsonConverter {
    /** */
    public static ConfigurationVisitor<JsonElement> jsonVisitor() {
        return new ConfigurationVisitor<JsonElement>() {
            /** */
            private final Deque<JsonObject> jsonObjectsStack = new ArrayDeque<>();

            /** {@inheritDoc} */
            @Override public JsonElement visitLeafNode(String key, Serializable val) {
                JsonElement jsonLeaf = toJsonLeaf(val);

                addToParent(key, jsonLeaf);

                return jsonLeaf;
            }

            /** {@inheritDoc} */
            @Override public JsonElement visitInnerNode(String key, InnerNode node) {
                JsonObject innerJsonNode = new JsonObject();

                jsonObjectsStack.push(innerJsonNode);

                node.traverseChildren(this);

                jsonObjectsStack.pop();

                addToParent(key, innerJsonNode);

                return innerJsonNode;
            }

            /** {@inheritDoc} */
            @Override public <N extends InnerNode> JsonElement visitNamedListNode(String key, NamedListNode<N> node) {
                JsonObject namedListJsonNode = new JsonObject();

                jsonObjectsStack.push(namedListJsonNode);

                for (String subkey : node.namedListKeys())
                    node.get(subkey).accept(subkey, this);

                jsonObjectsStack.pop();

                addToParent(key, namedListJsonNode);

                return namedListJsonNode;
            }

            /** */
            @NotNull private JsonElement toJsonLeaf(Serializable val) {
                if (val == null)
                    return JsonNull.INSTANCE;

                Class<? extends Serializable> valClass = val.getClass();

                if (!valClass.isArray())
                    return toJsonPrimitive(val);

                int size = Array.getLength(val);

                JsonArray jsonArray = new JsonArray(size);

                for (int idx = 0; idx < size; idx++)
                    jsonArray.add(toJsonPrimitive(Array.get(val, idx)));

                return jsonArray;
            }

            /** */
            @NotNull private JsonElement toJsonPrimitive(Object val) {
                if (val == null)
                    return JsonNull.INSTANCE;

                if (val instanceof Boolean)
                    return new JsonPrimitive((Boolean)val);

                if (val instanceof String)
                    return new JsonPrimitive((String)val);

                assert val instanceof Number : val.getClass();

                return new JsonPrimitive((Number)val);
            }

            /**
             * Add subelement to the paretn JSON object if it exists.
             *
             * @param key Key for the passed JSON element.
             * @param jsonElement JSON element to add to the parent.
             */
            private void addToParent(String key, JsonElement jsonElement) {
                if (!jsonObjectsStack.isEmpty())
                    jsonObjectsStack.peek().add(key, jsonElement);
            }
        };
    }

    /**
     * @param jsonElement JSON that has to be converted to the configuration source.
     * @return JSON-based configuration source.
     */
    public static ConfigurationSource jsonSource(JsonElement jsonElement) {
        if (!jsonElement.isJsonObject())
            throw new IllegalArgumentException("JSON object is expected as a configuration source");

        return new JsonObjectConfigurationSource(new ArrayList<>(), jsonElement.getAsJsonObject());
    }

    private static class JsonObjectConfigurationSource implements ConfigurationSource {
        /** */
        private final List<String> path;

        /** */
        private final JsonObject jsonObject;

        private JsonObjectConfigurationSource(List<String> path, JsonObject jsonObject) {
            this.path = path;
            this.jsonObject = jsonObject;
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> clazz) {
            throw new IllegalArgumentException(
                "'" + clazz.getSimpleName() + "' is expected as a type for '"
                    + join(path) + "' configuration value"
            );
        }

        /** {@inheritDoc} */
        @Override public void descend(ConstructableTreeNode node) {
            for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                String key = entry.getKey();

                JsonElement jsonElement = entry.getValue();

                try {
                    if (jsonElement.isJsonArray() || jsonElement.isJsonPrimitive()) {
                        List<String> path = new ArrayList<>(this.path.size() + 1);
                        path.addAll(this.path);
                        path.add(key);

                        node.construct(key, new JsonPrimitiveConfigurationSource(path, jsonElement));
                    }
                    else if (jsonElement.isJsonNull()) {
                        node.construct(key, null);
                    }
                    else {
                        assert jsonElement.isJsonObject();

                        List<String> path = new ArrayList<>(this.path.size() + 1);
                        path.addAll(this.path);
                        path.add(key);

                        node.construct(key, new JsonObjectConfigurationSource(path, jsonElement.getAsJsonObject()));
                    }
                }
                catch (NoSuchElementException e) {
                    if (path.isEmpty()) {
                        throw new IllegalArgumentException(
                            "'" + key + "' configuration root doesn't exist"
                        );
                    }
                    else {
                        throw new IllegalArgumentException(
                            "'" + join(path) + "' configuration doesn't have '" + key + "' subconfiguration"
                        );
                    }
                }
            }
        }
    }

    private static class JsonPrimitiveConfigurationSource implements ConfigurationSource {
        private final List<String> path;

        private final JsonElement jsonLeaf;

        private JsonPrimitiveConfigurationSource(List<String> path, JsonElement jsonLeaf) {
            assert !path.isEmpty();

            this.path = path;
            this.jsonLeaf = jsonLeaf;
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(Class<T> clazz) {
            if (clazz.isArray()) {
                if (!jsonLeaf.isJsonArray())
                    throw wrongTypeException(clazz, -1);

                JsonArray jsonArray = jsonLeaf.getAsJsonArray();
                int size = jsonArray.size();

                Class<?> componentType = clazz.getComponentType();
                Class<?> boxedComponentType = box(componentType);

                Object resArray = Array.newInstance(componentType, size);

                for (int idx = 0; idx < size; idx++) {
                    JsonElement element = jsonArray.get(idx);

                    if (!element.isJsonPrimitive())
                        throw wrongTypeException(boxedComponentType, idx);

                    Array.set(resArray, idx, unwrap(element.getAsJsonPrimitive(), boxedComponentType, idx));
                }

                return (T)resArray;
            }
            else {
                if (jsonLeaf.isJsonArray())
                    throw wrongTypeException(clazz, -1);

                return unwrap(jsonLeaf.getAsJsonPrimitive(), clazz, -1);
            }
        }

        /** {@inheritDoc} */
        @Override public void descend(ConstructableTreeNode node) {
            throw new IllegalArgumentException(
                "'" + join(path) + "' is expected to be a composite configuration node, not a single value"
            );
        }

        @NotNull private <T> IllegalArgumentException wrongTypeException(Class<T> clazz, int idx) {
            return new IllegalArgumentException(
                "'" + unbox(clazz).getSimpleName() + "' is expected as a type for '"
                    + join(path) + (idx == -1 ? "" : ("[" + idx + "]")) + "' configuration value"
            );
        }

        /**
         * @param clazz Class object.
         * @return Same object of it doesn't represent primitive class, boxed version otherwise.
         */
        private static Class<?> box(Class<?> clazz) {
            if (!clazz.isPrimitive())
                return clazz;

            switch (clazz.getName()) {
                case "boolean":
                    return Boolean.class;

                case "int":
                    return Integer.class;

                case "long":
                    return Long.class;

                default:
                    assert clazz == double.class;

                    return Double.class;
            }
        }

        private static Class<?> unbox(Class<?> clazz) {
            assert !clazz.isPrimitive();

            if (clazz == Boolean.class)
                return boolean.class;
            else if (clazz == Integer.class)
                return int.class;
            else if (clazz == Long.class)
                return long.class;
            else if (clazz == Double.class)
                return double.class;
            else
                return clazz;
        }

        private <T> T unwrap(JsonPrimitive jsonPrimitive, Class<T> clazz, int idx) {
            assert !clazz.isArray();
            assert !clazz.isPrimitive();

            if (clazz == String.class) {
                if (!jsonPrimitive.isString())
                    throw wrongTypeException(clazz, idx);

                return clazz.cast(jsonPrimitive.getAsString());
            }

            if (Number.class.isAssignableFrom(clazz)) {
                if (!jsonPrimitive.isNumber())
                    throw wrongTypeException(clazz, idx);

                if (clazz == Double.class)
                    return clazz.cast(jsonPrimitive.getAsDouble());

                if (clazz == Long.class)
                    return clazz.cast(jsonPrimitive.getAsLong());

                assert clazz == Integer.class;

                long longValue = jsonPrimitive.getAsLong();

                if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException(
                        "'" + join(path) + "' has integer type"
                            + " and the value " + longValue + " is out of bounds"
                    );
                }

                return clazz.cast((int)longValue);
            }

            assert clazz == Boolean.class;

            if (!jsonPrimitive.isBoolean())
                throw wrongTypeException(clazz, idx);

            return clazz.cast(jsonPrimitive.getAsBoolean());
        }
    }
}

