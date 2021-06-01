/*
 * Copyright 2013 Aggregate Knowledge, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.stat.hll.serialization;

import org.apache.ignite.internal.processors.query.stat.hll.HLLType;

/**
 * A collection of constants and utilities for serializing and deserializing
 * HLLs.
 *
 * NOTE:  'package' visibility is used for many methods that only need to be
 *        used by the {@link ISchemaVersion} implementations. The structure of
 *        a serialized HLL's metadata should be opaque to the rest of the
 *        library.
 *
 * @author timon
 */
public class SerializationUtil {
    /**
     * The number of bits (of the parameters byte) dedicated to encoding the
     * width of the registers.
     */
    /*package*/ static int REGISTER_WIDTH_BITS = 3;

    /**
     * A mask to cap the maximum value of the register width.
     */
    /*package*/ static int REGISTER_WIDTH_MASK = (1 << REGISTER_WIDTH_BITS) - 1;

    /**
     * The number of bits (of the parameters byte) dedicated to encoding
     * <code>log2(registerCount)</code>.
     */
    /*package*/ static int LOG2_REGISTER_COUNT_BITS = 5;

    /**
     * A mask to cap the maximum value of <code>log2(registerCount)</code>.
     */
    /*package*/ static int LOG2_REGISTER_COUNT_MASK = (1 << LOG2_REGISTER_COUNT_BITS) - 1;

    /**
     * The number of bits (of the cutoff byte) dedicated to encoding the
     * log-base-2 of the explicit cutoff or sentinel values for
     * 'explicit-disabled' or 'auto'.
     */
    /*package*/ static int EXPLICIT_CUTOFF_BITS = 6;

    /**
     * A mask to cap the maximum value of the explicit cutoff choice.
     */
    /*package*/ static int EXPLICIT_CUTOFF_MASK = (1 << EXPLICIT_CUTOFF_BITS) - 1;

    /**
     * Number of bits in a nibble.
     */
    private static int NIBBLE_BITS = 4;

    /**
     * A mask to cap the maximum value of a nibble.
     */
    private static int NIBBLE_MASK = (1 << NIBBLE_BITS) - 1;

    // ************************************************************************
    // Serialization utilities

    /**
     * Schema version one (v1).
     */
    public static ISchemaVersion VERSION_ONE = new SchemaVersionOne();

    /**
     * The default schema version for serializing HLLs.
     */
    public static ISchemaVersion DEFAULT_SCHEMA_VERSION = VERSION_ONE;

    /**
     * List of registered schema versions, indexed by their version numbers. If
     * an entry is <code>null</code>, then no such schema version is registered.
     * Similarly, registering a new schema version simply entails assigning an
     * {@link ISchemaVersion} instance to the appropriate index of this array.<p/>
     *
     * By default, only {@link SchemaVersionOne} is registered. Note that version
     * zero will always be reserved for internal (e.g. proprietary, legacy) schema
     * specifications/implementations and will never be assigned to in by this
     * library.
     */
    public static ISchemaVersion[] REGISTERED_SCHEMA_VERSIONS = new ISchemaVersion[16];

    static {
        REGISTERED_SCHEMA_VERSIONS[1] = VERSION_ONE;
    }

    /**
     * @param  schemaVersionNumber the version number of the {@link ISchemaVersion}
     *         desired. This must be a registered schema version number.
     * @return The {@link ISchemaVersion} for the given number. This will never
     *         be <code>null</code>.
     */
    public static ISchemaVersion getSchemaVersion(final int schemaVersionNumber) {
        if(schemaVersionNumber >= REGISTERED_SCHEMA_VERSIONS.length || schemaVersionNumber < 0) {
            throw new RuntimeException("Invalid schema version number " + schemaVersionNumber);
        }
        final ISchemaVersion schemaVersion = REGISTERED_SCHEMA_VERSIONS[schemaVersionNumber];
        if(schemaVersion == null) {
            throw new RuntimeException("Unknown schema version number " + schemaVersionNumber);
        }
        return schemaVersion;
    }

    /**
     * Get the appropriate {@link ISchemaVersion schema version} for the specified
     * serialized HLL.
     *
     * @param  bytes the serialized HLL whose schema version is desired.
     * @return the schema version for the specified HLL. This will never
     *         be <code>null</code>.
     */
    public static ISchemaVersion getSchemaVersion(final byte[] bytes) {
        final byte versionByte = bytes[0];
        final int schemaVersionNumber = schemaVersion(versionByte);

        return getSchemaVersion(schemaVersionNumber);
    }

    // ************************************************************************
    // Package-specific shared helpers

    /**
     * Generates a byte that encodes the schema version and the type ordinal
     * of the HLL.
     *
     * The top nibble is the schema version and the bottom nibble is the type
     * ordinal.
     *
     * @param schemaVersion the schema version to encode.
     * @param typeOrdinal the type ordinal of the HLL to encode.
     * @return the packed version byte
     */
    public static byte packVersionByte(final int schemaVersion, final int typeOrdinal) {
        return (byte)(((NIBBLE_MASK & schemaVersion) << NIBBLE_BITS) | (NIBBLE_MASK & typeOrdinal));
    }
    /**
     * Generates a byte that encodes the log-base-2 of the explicit cutoff
     * or sentinel values for 'explicit-disabled' or 'auto', as well as the
     * boolean indicating whether to use {@link HLLType#SPARSE}
     * in the promotion hierarchy.
     *
     * The top bit is always padding, the second highest bit indicates the
     * 'sparse-enabled' boolean, and the lowest six bits encode the explicit
     * cutoff value.
     *
     * @param  explicitCutoff the explicit cutoff value to encode.
     *         <ul>
     *           <li>
     *             If 'explicit-disabled' is chosen, this value should be <code>0</code>.
     *           </li>
     *           <li>
     *             If 'auto' is chosen, this value should be <code>63</code>.
     *           </li>
     *           <li>
     *             If a cutoff of 2<sup>n</sup> is desired, for <code>0 <= n < 31</code>,
     *             this value should be <code>n + 1</code>.
     *           </li>
     *         </ul>
     * @param  sparseEnabled whether {@link HLLType#SPARSE}
     *         should be used in the promotion hierarchy to improve HLL
     *         storage.
     *
     * @return the packed cutoff byte
     */
    public static byte packCutoffByte(final int explicitCutoff, final boolean sparseEnabled) {
        final int sparseBit = (sparseEnabled ? (1 << EXPLICIT_CUTOFF_BITS) : 0);
        return (byte)(sparseBit | (EXPLICIT_CUTOFF_MASK & explicitCutoff));
    }

    /**
     * Generates a byte that encodes the parameters of a
     * {@link HLLType#FULL} or {@link HLLType#SPARSE}
     * HLL.<p/>
     *
     * The top 3 bits are used to encode <code>registerWidth - 1</code>
     * (range of <code>registerWidth</code> is thus 1-9) and the bottom 5
     * bits are used to encode <code>registerCountLog2</code>
     * (range of <code>registerCountLog2</code> is thus 0-31).
     *
     * @param  registerWidth the register width (must be at least 1 and at
     *         most 9)
     * @param  registerCountLog2 the log-base-2 of the register count (must
     *         be at least 0 and at most 31)
     * @return the packed parameters byte
     */
    public static byte packParametersByte(final int registerWidth, final int registerCountLog2) {
        final int widthBits = ((registerWidth - 1) & REGISTER_WIDTH_MASK);
        final int countBits = (registerCountLog2 & LOG2_REGISTER_COUNT_MASK);
        return (byte)((widthBits << LOG2_REGISTER_COUNT_BITS) | countBits);
    }

    /**
     * Extracts the 'sparse-enabled' boolean from the cutoff byte of a serialized
     * HLL.
     *
     * @param  cutoffByte the cutoff byte of the serialized HLL
     * @return the 'sparse-enabled' boolean
     */
    public static boolean sparseEnabled(final byte cutoffByte) {
        return ((cutoffByte >>> EXPLICIT_CUTOFF_BITS) & 1) == 1;
    }

    /**
     * Extracts the explicit cutoff value from the cutoff byte of a serialized
     * HLL.
     *
     * @param  cutoffByte the cutoff byte of the serialized HLL
     * @return the explicit cutoff value
     */
    public static int explicitCutoff(final byte cutoffByte) {
        return (cutoffByte & EXPLICIT_CUTOFF_MASK);
    }

    /**
     * Extracts the schema version from the version byte of a serialized
     * HLL.
     *
     * @param  versionByte the version byte of the serialized HLL
     * @return the schema version of the serialized HLL
     */
    public static int schemaVersion(final byte versionByte) {
        return NIBBLE_MASK & (versionByte >>> NIBBLE_BITS);
    }

    /**
     * Extracts the type ordinal from the version byte of a serialized HLL.
     *
     * @param  versionByte the version byte of the serialized HLL
     * @return the type ordinal of the serialized HLL
     */
    public static int typeOrdinal(final byte versionByte) {
        return (versionByte & NIBBLE_MASK);
    }

    /**
     * Extracts the register width from the parameters byte of a serialized
     * {@link HLLType#FULL} HLL.
     *
     * @param  parametersByte the parameters byte of the serialized HLL
     * @return the register width of the serialized HLL
     *
     * @see #packParametersByte(int, int)
     */
    public static int registerWidth(final byte parametersByte) {
        return ((parametersByte >>> LOG2_REGISTER_COUNT_BITS) & REGISTER_WIDTH_MASK) + 1;
    }

    /**
     * Extracts the log2(registerCount) from the parameters byte of a
     * serialized {@link HLLType#FULL} HLL.
     *
     * @param  parametersByte the parameters byte of the serialized HLL
     * @return log2(registerCount) of the serialized HLL
     *
     * @see #packParametersByte(int, int)
     */
    public static int registerCountLog2(final byte parametersByte) {
        return (parametersByte & LOG2_REGISTER_COUNT_MASK);
    }
}
