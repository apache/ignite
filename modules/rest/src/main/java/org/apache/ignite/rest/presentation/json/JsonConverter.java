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

package org.apache.ignite.rest.presentation.json;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.Reader;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.apache.ignite.rest.presentation.FormatConverter;
import org.jetbrains.annotations.NotNull;

/** */
public class JsonConverter implements FormatConverter {
    /** */
    private final Gson gson = new Gson();

    /** {@inheritDoc} */
    @Override public String convertTo(Object obj) {
        return gson.toJson(obj);
    }

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

                JsonArray jsonArray = new JsonArray();

                for (int i = 0; i < Array.getLength(val); i++)
                    jsonArray.add(toJsonPrimitive(Array.get(val, i)));

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

                if (val instanceof Number)
                    return new JsonPrimitive((Number)val);

                assert false : val;

                throw new IllegalArgumentException(val.getClass().getCanonicalName());
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

    /** */
    public static ConfigurationSource jsonSource(JsonElement jsonElement) {
        //TODO IGNITE-14372 Finish this implementation.
        return null;
    }

    private static class JsonObjectConfigurationSource implements ConfigurationSource {
        /** Shared. */
        private final List<String> path;

        /** */
        private final JsonObject jsonObject;

        private JsonObjectConfigurationSource(List<String> path, JsonObject jsonObject) {
            this.path = path;
            this.jsonObject = jsonObject;
        }

        @Override public <T> T unwrap(Class<T> clazz) {
            throw new IllegalArgumentException(""); //TODO IGNITE-14372 Implement.
        }

        @Override public void descend(ConstructableTreeNode node) {
            for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                String key = entry.getKey();

                path.add(key);

                JsonElement jsonElement = entry.getValue();

                try {
                    if (jsonElement.isJsonArray() || jsonElement.isJsonPrimitive())
                        node.construct(key, new JsonPrimitiveConfigurationSource(path, jsonElement));
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
                    throw new IllegalArgumentException(""); //TODO IGNITE-14372 Update comment.
                }

                path.remove(path.size() - 1);
            }
        }
    }

    private static class JsonPrimitiveConfigurationSource implements ConfigurationSource {
        private final List<String> path;

        private final JsonElement jsonLeaf;

        private JsonPrimitiveConfigurationSource(List<String> path, JsonElement jsonLeaf) {
            this.path = path;
            this.jsonLeaf = jsonLeaf;
        }

        @Override public <T> T unwrap(Class<T> clazz) {
            if (clazz.isArray() != jsonLeaf.isJsonArray())
                throw new IllegalArgumentException(""); //TODO IGNITE-14372 Update comment.

            return null;
        }

        @Override public void descend(ConstructableTreeNode node) {
            throw new IllegalArgumentException(""); //TODO IGNITE-14372 Update comment.
        }

        private <T> T unwrap(JsonPrimitive jsonPrimitive, Class<T> clazz) {
            assert !clazz.isArray();

            if (clazz == String.class) {
                if (!jsonPrimitive.isString())
                    throw new IllegalArgumentException(""); //TODO IGNITE-14372 Update comment.

                return clazz.cast(jsonPrimitive.getAsString());
            }

            if (Number.class.isAssignableFrom(clazz)) {
                if (!jsonPrimitive.isNumber())
                    throw new IllegalArgumentException(""); //TODO IGNITE-14372 Update comment.

                if (clazz == Double.class)
                    return clazz.cast(jsonPrimitive.getAsDouble());

                if (clazz == Long.class)
                    return clazz.cast(jsonPrimitive.getAsLong());

                if (clazz == Integer.class) {
                    long longValue = jsonPrimitive.getAsLong();

                    if (longValue < Integer.MIN_VALUE || longValue > Integer.MAX_VALUE)
                        throw new IllegalArgumentException(""); //TODO IGNITE-14372 Update comment.

                    return clazz.cast((int)longValue);
                }

                throw new AssertionError(clazz);
            }

            if (clazz == Boolean.class) {
                if (!jsonPrimitive.isBoolean())
                    throw new IllegalArgumentException(""); //TODO IGNITE-14372 Update comment.

                return clazz.cast(jsonPrimitive.getAsBoolean());
            }

            throw new IllegalArgumentException(""); //TODO IGNITE-14372 Update comment.
        }
    }

    /** {@inheritDoc} */
    @Override public String convertTo(String rootName, Object src) {
        Map<String, Object> res = new HashMap<>();

        res.put(rootName, src);

        return gson.toJson(res);
    }

    /** {@inheritDoc} */
    @Override public String rootName(String source) {
        Map<String, Object> map = gson.fromJson(source, Map.class);

        // Peek only first root for simplicity. See comment in ConfigurationPresentation#update for more context.
        Optional<String> firstOpt = map.keySet().stream().findFirst();

        return firstOpt.isPresent() ? firstOpt.get() : null;
    }

    /** {@inheritDoc} */
    @Override public Object convertFrom(String source, String rootName, Class<?> clazz) {
        Map map = gson.fromJson(source, Map.class);

        String root = gson.toJson(map.get(rootName));

        return gson.fromJson(root, clazz);
    }

    /** {@inheritDoc} */
    @Override public <T> T convertFrom(Reader source, String rootName, Class<T> clazz) {
        Map map = gson.fromJson(source, Map.class);

        String root = gson.toJson(map.get(rootName));

        return gson.fromJson(root, clazz);
    }
}
