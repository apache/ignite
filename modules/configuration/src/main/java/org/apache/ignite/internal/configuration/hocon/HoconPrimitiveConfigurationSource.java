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

package org.apache.ignite.internal.configuration.hocon;

import static com.typesafe.config.ConfigValueType.BOOLEAN;
import static com.typesafe.config.ConfigValueType.NUMBER;
import static com.typesafe.config.ConfigValueType.STRING;
import static java.lang.String.format;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.join;

import com.typesafe.config.ConfigValue;
import java.util.List;
import org.apache.ignite.internal.configuration.TypeUtils;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;

/**
 * {@link ConfigurationSource} created from a HOCON element representing a primitive type.
 */
class HoconPrimitiveConfigurationSource implements ConfigurationSource {
    /**
     * Current path inside the top-level HOCON object.
     */
    private final List<String> path;

    /**
     * HOCON object that this source has been created from.
     */
    private final ConfigValue hoconCfgValue;

    /**
     * Creates a {@link ConfigurationSource} from the given HOCON object representing a primitive type.
     *
     * @param path          current path inside the top-level HOCON object. Can be empty if the given {@code hoconCfgValue} is the top-level
     *                      object
     * @param hoconCfgValue HOCON object
     */
    HoconPrimitiveConfigurationSource(List<String> path, ConfigValue hoconCfgValue) {
        assert !path.isEmpty();

        this.path = path;
        this.hoconCfgValue = hoconCfgValue;
    }

    /** {@inheritDoc} */
    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isArray()) {
            throw wrongTypeException(clazz, path, -1);
        }

        return unwrapPrimitive(hoconCfgValue, clazz, path, -1);
    }

    /** {@inheritDoc} */
    @Override
    public void descend(ConstructableTreeNode node) {
        throw new IllegalArgumentException(
                format("'%s' is expected to be a composite configuration node, not a single value", join(path))
        );
    }

    /**
     * Returns exception with the message that a value is expected to be of a specific type.
     *
     * @param clazz Expected type of the value.
     * @param path  Path to the value.
     * @param idx   Index in the array if the value is an array element. {@code -1} if it's not.
     * @return New {@link IllegalArgumentException} instance.
     */
    public static IllegalArgumentException wrongTypeException(Class<?> clazz, List<String> path, int idx) {
        return new IllegalArgumentException(format(
                "'%s' is expected as a type for the '%s' configuration value",
                unbox(clazz).getSimpleName(), formatArrayPath(path, idx)
        ));
    }

    /**
     * Non-null wrapper over {@link TypeUtils#unboxed}.
     *
     * @param clazz Class for non-primitive objects.
     * @return Unboxed class fox boxed classes, same object otherwise.
     */
    private static Class<?> unbox(Class<?> clazz) {
        assert !clazz.isPrimitive();

        Class<?> unboxed = TypeUtils.unboxed(clazz);

        return unboxed == null ? clazz : unboxed;
    }

    /**
     * Extracts the value from the given {@link ConfigValue} based on the expected type.
     *
     * @param hoconCfgValue Configuration value to unwrap.
     * @param clazz         Class that signifies resulting type of the value. Boxed primitive or String.
     * @param path          Path to the value, used for error messages.
     * @param idx           Index in the array if the value is an array element. {@code -1} if it's not.
     * @param <T>           Type of the resulting unwrapped object.
     * @return Unwrapped object.
     * @throws IllegalArgumentException In case of type mismatch or numeric overflow.
     */
    public static <T> T unwrapPrimitive(ConfigValue hoconCfgValue, Class<T> clazz, List<String> path, int idx) {
        assert !clazz.isArray();
        assert !clazz.isPrimitive();

        if (clazz == String.class) {
            if (hoconCfgValue.valueType() != STRING) {
                throw wrongTypeException(clazz, path, idx);
            }

            return clazz.cast(hoconCfgValue.unwrapped());
        } else if (clazz == Boolean.class) {
            if (hoconCfgValue.valueType() != BOOLEAN) {
                throw wrongTypeException(clazz, path, idx);
            }

            return clazz.cast(hoconCfgValue.unwrapped());
        } else if (clazz == Character.class) {
            if (hoconCfgValue.valueType() != STRING || hoconCfgValue.unwrapped().toString().length() != 1) {
                throw wrongTypeException(clazz, path, idx);
            }

            return clazz.cast(hoconCfgValue.unwrapped().toString().charAt(0));
        } else if (Number.class.isAssignableFrom(clazz)) {
            if (hoconCfgValue.valueType() != NUMBER) {
                throw wrongTypeException(clazz, path, idx);
            }

            Number numberValue = (Number) hoconCfgValue.unwrapped();

            if (clazz == Byte.class) {
                checkBounds(numberValue, Byte.MIN_VALUE, Byte.MAX_VALUE, path, idx);

                return clazz.cast(numberValue.byteValue());
            } else if (clazz == Short.class) {
                checkBounds(numberValue, Short.MIN_VALUE, Short.MAX_VALUE, path, idx);

                return clazz.cast(numberValue.shortValue());
            } else if (clazz == Integer.class) {
                checkBounds(numberValue, Integer.MIN_VALUE, Integer.MAX_VALUE, path, idx);

                return clazz.cast(numberValue.intValue());
            } else if (clazz == Long.class) {
                return clazz.cast(numberValue.longValue());
            } else if (clazz == Float.class) {
                return clazz.cast(numberValue.floatValue());
            } else if (clazz == Double.class) {
                return clazz.cast(numberValue.doubleValue());
            }
        }

        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }

    /**
     * Checks that a number fits into the given bounds.
     *
     * @param numberValue Number value to validate.
     * @param lower       Lower bound, inclusive.
     * @param upper       Upper bound, inclusive.
     * @param path        Path to the value, used for error messages.
     * @param idx         Index in the array if the value is an array element. {@code -1} if it's not.
     */
    private static void checkBounds(Number numberValue, long lower, long upper, List<String> path, int idx) {
        long longValue = numberValue.longValue();

        if (longValue < lower || longValue > upper) {
            throw new IllegalArgumentException(format(
                    "Value '%d' of '%s' is out of its declared bounds: [%d : %d]",
                    longValue, formatArrayPath(path, idx), lower, upper
            ));
        }
    }

    /**
     * Creates a string representation of the current HOCON path inside of an array.
     *
     * @param path Path to the value.
     * @param idx  Index in the array if the value is an array element. {@code -1} if it's not.
     * @return Path in a proper format.
     */
    public static String formatArrayPath(List<String> path, int idx) {
        return join(path) + (idx == -1 ? "" : ("[" + idx + "]"));
    }
}
