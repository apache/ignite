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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.join;

/**
 * {@link ConfigurationSource} created from a JSON object.
 */
class JsonObjectConfigurationSource implements ConfigurationSource {
    /**
     * Current path inside the top-level JSON object.
     */
    private final List<String> path;

    /**
     * JSON object that this source has been created from.
     */
    private final JsonObject jsonObject;

    /**
     * Creates a {@link ConfigurationSource} from the given JSON object.
     *
     * @param path current path inside the top-level JSON object. Can be empty if the given {@code jsonObject}
     *             is the top-level object
     * @param jsonObject JSON object
     */
    JsonObjectConfigurationSource(List<String> path, JsonObject jsonObject) {
        this.path = path;
        this.jsonObject = jsonObject;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        throw new IllegalArgumentException(
            String.format("'%s' is expected as a type for the '%s' configuration value", clazz.getSimpleName(), join(path))
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
                else if (jsonElement.isJsonNull())
                    node.construct(key, null);
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
                        String.format("'%s' configuration root doesn't exist", key), e
                    );
                }
                else {
                    throw new IllegalArgumentException(
                        String.format("'%s' configuration doesn't have the '%s' sub-configuration", join(path), key), e
                    );
                }
            }
        }
    }
}
