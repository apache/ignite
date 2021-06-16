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

package org.apache.ignite.internal.configuration.json;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.Deque;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ConfigurationVisitor} implementation that converts a configuration tree to a JSON representation.
 */
class JsonConfigurationVisitor implements ConfigurationVisitor<JsonElement> {
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
    @NotNull private static JsonElement toJsonLeaf(Serializable val) {
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
    @NotNull private static JsonElement toJsonPrimitive(Object val) {
        if (val == null)
            return JsonNull.INSTANCE;
        else if (val instanceof Boolean)
            return new JsonPrimitive((Boolean)val);
        else if (val instanceof Character)
            return new JsonPrimitive((Character)val);
        else if (val instanceof String)
            return new JsonPrimitive((String)val);
        else if (val instanceof Number)
            return new JsonPrimitive((Number)val);
        else
            throw new IllegalArgumentException("Unsupported type: " + val.getClass());
    }

    /**
     * Adds a sub-element to the parent JSON object if it exists.
     *
     * @param key key for the passed JSON element
     * @param jsonElement JSON element to add to the parent
     */
    private void addToParent(String key, JsonElement jsonElement) {
        if (!jsonObjectsStack.isEmpty())
            jsonObjectsStack.peek().add(key, jsonElement);
    }
}
