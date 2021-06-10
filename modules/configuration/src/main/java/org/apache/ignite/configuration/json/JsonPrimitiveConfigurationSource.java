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

package org.apache.ignite.configuration.json;

import java.lang.reflect.Array;
import java.util.List;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.apache.ignite.configuration.TypeUtils;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConstructableTreeNode;

import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.join;

/**
 * {@link ConfigurationSource} created from a JSON element representing a primitive type.
 */
class JsonPrimitiveConfigurationSource implements ConfigurationSource {
    /**
     * Current path inside the top-level JSON object.
     */
    private final List<String> path;

    /**
     * JSON object that this source has been created from.
     */
    private final JsonElement jsonLeaf;

    /**
     * Creates a {@link ConfigurationSource} from the given JSON object representing a primitive type.
     *
     * @param path current path inside the top-level JSON object. Can be empty if the given {@code jsonObject}
     *             is the top-level object
     * @param jsonLeaf JSON object
     */
    JsonPrimitiveConfigurationSource(List<String> path, JsonElement jsonLeaf) {
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

    /** */
    private IllegalArgumentException wrongTypeException(Class<?> clazz, int idx) {
        return new IllegalArgumentException(String.format(
            "'%s' is expected as a type for the '%s' configuration value",
            unbox(clazz).getSimpleName(), formatArrayPath(idx)
        ));
    }

    /**
     * Non-null wrapper over {@link TypeUtils#boxed}.
     */
    private static Class<?> box(Class<?> clazz) {
        Class<?> boxed = TypeUtils.boxed(clazz);

        return boxed == null ? clazz : boxed;
    }

    /**
     * Non-null wrapper over {@link TypeUtils#unboxed}.
     */
    private static Class<?> unbox(Class<?> clazz) {
        assert !clazz.isPrimitive();

        Class<?> unboxed = TypeUtils.unboxed(clazz);

        return unboxed == null ? clazz : unboxed;
    }

    /**
     * Extracts the value from the given {@link JsonPrimitive} based on the expected type.
     */
    private <T> T unwrap(JsonPrimitive jsonPrimitive, Class<T> clazz, int idx) {
        assert !clazz.isArray();
        assert !clazz.isPrimitive();

        if (clazz == String.class) {
            if (!jsonPrimitive.isString())
                throw wrongTypeException(clazz, idx);

            return clazz.cast(jsonPrimitive.getAsString());
        }
        else if (clazz == Boolean.class) {
            if (!jsonPrimitive.isBoolean())
                throw wrongTypeException(clazz, idx);

            return clazz.cast(jsonPrimitive.getAsBoolean());
        }
        else if (clazz == Character.class) {
            if (!jsonPrimitive.isString() || jsonPrimitive.getAsString().length() != 1)
                throw wrongTypeException(clazz, idx);

            return clazz.cast(jsonPrimitive.getAsCharacter());
        }
        else if (Number.class.isAssignableFrom(clazz)) {
            if (!jsonPrimitive.isNumber())
                throw wrongTypeException(clazz, idx);

            if (clazz == Byte.class) {
                checkBounds(jsonPrimitive, Byte.MIN_VALUE, Byte.MAX_VALUE, idx);

                return clazz.cast(jsonPrimitive.getAsByte());
            }
            else if (clazz == Short.class) {
                checkBounds(jsonPrimitive, Short.MIN_VALUE, Short.MAX_VALUE, idx);

                return clazz.cast(jsonPrimitive.getAsShort());
            }
            else if (clazz == Integer.class) {
                checkBounds(jsonPrimitive, Integer.MIN_VALUE, Integer.MAX_VALUE, idx);

                return clazz.cast(jsonPrimitive.getAsInt());
            }
            else if (clazz == Long.class)
                return clazz.cast(jsonPrimitive.getAsLong());
            else if (clazz == Float.class)
                return clazz.cast(jsonPrimitive.getAsFloat());
            else if (clazz == Double.class)
                return clazz.cast(jsonPrimitive.getAsDouble());
        }

        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }

    /**
     * Checks that a numeric JSON primitive fits into the given bounds.
     */
    private void checkBounds(JsonPrimitive jsonPrimitive, long lower, long upper, int idx) {
        long longValue = jsonPrimitive.getAsLong();

        if (longValue < lower || longValue > upper) {
            throw new IllegalArgumentException(String.format(
                "Value '%d' of '%s' is out of its declared bounds: [%d : %d]",
                longValue, formatArrayPath(idx), lower, upper
            ));
        }
    }

    /**
     * Creates a string representation of the current JSON path inside of an array.
     */
    private String formatArrayPath(int idx) {
        return join(path) + (idx == -1 ? "" : ("[" + idx + "]"));
    }
}
