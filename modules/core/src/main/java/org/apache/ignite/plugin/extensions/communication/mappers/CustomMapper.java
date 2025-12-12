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

package org.apache.ignite.plugin.extensions.communication.mappers;

/**
 * An interface to implement custom serialization and deserialization of enum values used in network communication.
 *
 * <p>This interface allows users to define a custom mapping between enum constants and their {@code byte}
 * representations sent over the wire, instead of relying on the default ordinal-based encoding.
 * It can be used in conjunction with the {@code org.apache.ignite.internal.CustomMapper} annotation
 * to plug in user-defined mapping logic during generation of serialization code.</p>
 *
 * <p>Implementations must ensure that:
 * <ul>
 *     <li>Each enum constant maps to a unique {@code byte} value.</li>
 *     <li>The {@code byte} codes are stable and consistent across all nodes in the cluster.</li>
 *     <li>The {@link #decode(byte)} method handles invalid or unknown codes appropriately
 *     (e.g., throws an exception or returns a default).</li>
 * </ul>
 * </p>
 *
 * <p>Example implementation:</p>
 * <pre>{@code
 * public class MyColorMapper implements CustomMapper<Color> {
 *     public byte encode(Color color) {
 *         switch (color) {
 *             case null:  return -1;
 *             case RED:   return 0;
 *             case GREEN: return 1;
 *             case BLUE:  return 2;
 *             default:    throw new IllegalArgumentException("Unknown color: " + color);
 *         }
 *     }
 *
 *     public Color decode(byte code) {
 *         switch (code) {
 *             case -1: return null;
 *             case 0: return Color.RED;
 *             case 1: return Color.GREEN;
 *             case 2: return Color.BLUE;
 *             default: throw new IllegalArgumentException("Unknown color code: " + code);
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p><strong>Note:</strong> This interface is used in Ignite's communication layer
 * to enable evolution of enum types between different versions of the software.</p>
 *
 * @param <T> The enum type for which this mapper provides encoding/decoding logic.
 * @see org.apache.ignite.internal.CustomMapper
 * @see DefaultEnumMapper
 */
public interface CustomMapper<T extends Enum<T>> {
    /**
     * Encodes given enum value into a {@code byte} representation to be sent over the network.
     *
     * @param val The enum value to encode; may be {@code null} depending on implementation contract.
     * @return The {@code byte} representation of the enum value.
     */
    public byte encode(T val);

    /**
     * Decodes a {@code byte} code received from the network into the corresponding enum constant.
     *
     * @param code The {@code byte} representation of the enum value.
     * @return The decoded enum value.
     * @throws IllegalArgumentException if the code does not correspond to any valid enum constant.
     */
    public T decode(byte code);
}
