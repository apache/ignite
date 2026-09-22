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
 * A default enum mapper that uses enum ordinal values to perform serialization and deserialization.
 *
 * <p>This mapper encodes an enum constant into a {@code byte} by taking its {@link Enum#ordinal()},
 * and decodes a {@code byte} value back into the corresponding enum constant using array indexing.
 * The encoding returns {@code -1} for {@code null} inputs, and decoding returns {@code null} for
 * negative codes. If a provided code is out of range (greater than or equal to the number of enum
 * constants), an {@link IllegalArgumentException} is thrown.</p>
 *
 * <p>This class assumes that:
 * <ul>
 *     <li>Enums have no more than 127 constants (to fit in a positive {@code byte}).</li>
 *     <li>The order of enum constants (i.e., their ordinals) is stable and not subject to change.</li>
 * </ul>
 * </p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * public enum Color {
 *     RED, GREEN, BLUE;
 * }
 *
 * Color[] values = Color.values();
 * byte code = DefaultEnumMapper.INSTANCE.encode(Color.RED); // Returns 0
 * Color color = DefaultEnumMapper.INSTANCE.decode(values, code); // Returns Color.RED
 * }</pre>
 *
 * <p><strong>Note:</strong> This class is thread-safe and uses a singleton pattern.
 * Use {@link #INSTANCE} to access the shared instance.</p>
 *
 * @see Enum#ordinal()
 */
public final class DefaultEnumMapper {
    /** */
    public static final DefaultEnumMapper INSTANCE = new DefaultEnumMapper();

    /** */
    private DefaultEnumMapper() {}

    /**
     * Encodes the given enum value into a {@code byte} using its ordinal.
     *
     * @param <T> The enum type.
     * @param enumVal The enum value to encode; may be {@code null}.
     * @return The {@code byte} representation of the enum value (non-negative) or {@code -1} if the value is {@code null}.
     */
    public <T extends Enum<T>> byte encode(T enumVal) {
        if (enumVal == null)
            return -1;

        return (byte)enumVal.ordinal();
    }

    /**
     * Decodes a {@code byte} code into the corresponding enum constant.
     *
     * @param <T> The enum type.
     * @param vals Array of all possible values of the enum type. Must not be {@code null}.
     * @param enumCode The {@code byte} representation of the enum value.
     * @return The corresponding enum constant, or {@code null} if {@code enumCode} is negative.
     * @throws IllegalArgumentException if {@code enumCode} is out of range
     * (i.e., greater than or equal to the length of {@code vals} array).
     */
    public <T extends Enum<T>> T decode(T[] vals, byte enumCode) {
        if (enumCode < 0)
            return null;

        if (enumCode > vals.length - 1)
            throw new IllegalArgumentException("Enum code " + enumCode + " is out of range for enum type " + vals[0].getClass().getName());

        return vals[enumCode];
    }
}
