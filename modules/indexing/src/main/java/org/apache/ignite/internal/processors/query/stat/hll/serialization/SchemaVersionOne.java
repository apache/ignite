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
 * A concrete {@link ISchemaVersion} representing schema version one.
 *
 * @author timon
 */
public class SchemaVersionOne implements ISchemaVersion {
    /**
     * The schema version number for this instance.
     */
    public static final int SCHEMA_VERSION = 1;

    // ------------------------------------------------------------------------
    // Version-specific ordinals (array position) for each of the HLL types
    private static final HLLType[] TYPE_ORDINALS = new HLLType[] {
        HLLType.UNDEFINED,
        HLLType.EMPTY,
        HLLType.EXPLICIT,
        HLLType.SPARSE,
        HLLType.FULL
    };

    // ------------------------------------------------------------------------
    // number of header bytes for all HLL types
    private static final int HEADER_BYTE_COUNT = 3;

    // sentinel values from the spec for explicit off and auto
    private static final int EXPLICIT_OFF = 0;

    /** */
    private static final int EXPLICIT_AUTO = 63;

    // ************************************************************************
    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.ISchemaVersion#paddingBytes(HLLType)
     */
    @Override public int paddingBytes(final HLLType type) {
        return HEADER_BYTE_COUNT;
    }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.ISchemaVersion#writeMetadata(byte[], IHLLMetadata)
     */
    @Override public void writeMetadata(final byte[] bytes, final IHLLMetadata metadata) {
        final HLLType type = metadata.HLLType();
        final int typeOrdinal = getOrdinal(type);

        final int explicitCutoffValue;
        if (metadata.explicitOff())
            explicitCutoffValue = EXPLICIT_OFF;
        else if (metadata.explicitAuto())
            explicitCutoffValue = EXPLICIT_AUTO;
        else
            explicitCutoffValue = metadata.log2ExplicitCutoff() + 1/*per spec*/;

        bytes[0] = SerializationUtil.packVersionByte(SCHEMA_VERSION, typeOrdinal);
        bytes[1] = SerializationUtil.packParametersByte(metadata.registerWidth(), metadata.registerCountLog2());
        bytes[2] = SerializationUtil.packCutoffByte(explicitCutoffValue, metadata.sparseEnabled());
    }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.ISchemaVersion#readMetadata(byte[])
     */
    @Override public IHLLMetadata readMetadata(final byte[] bytes) {
        final byte versionByte = bytes[0];
        final byte parametersByte = bytes[1];
        final byte cutoffByte = bytes[2];

        final int typeOrdinal = SerializationUtil.typeOrdinal(versionByte);
        final int explicitCutoffValue = SerializationUtil.explicitCutoff(cutoffByte);
        final boolean explicitOff = (explicitCutoffValue == EXPLICIT_OFF);
        final boolean explicitAuto = (explicitCutoffValue == EXPLICIT_AUTO);
        final int log2ExplicitCutoff = (explicitOff || explicitAuto) ? -1/*sentinel*/ : (explicitCutoffValue - 1/*per spec*/);

        return new HLLMetadata(SCHEMA_VERSION,
            getType(typeOrdinal),
            SerializationUtil.registerCountLog2(parametersByte),
            SerializationUtil.registerWidth(parametersByte),
            log2ExplicitCutoff,
            explicitOff,
            explicitAuto,
            SerializationUtil.sparseEnabled(cutoffByte));
    }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.ISchemaVersion#getSerializer(HLLType, int, int)
     */
    @Override public IWordSerializer getSerializer(HLLType type, int wordLength, int wordCount) {
        return new BigEndianAscendingWordSerializer(wordLength, wordCount, paddingBytes(type));
    }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.ISchemaVersion#getDeserializer(HLLType, int, byte[])
     */
    @Override public IWordDeserializer getDeserializer(HLLType type, int wordLength, byte[] bytes) {
        return new BigEndianAscendingWordDeserializer(wordLength, paddingBytes(type), bytes);
    }

    /* (non-Javadoc)
     * @see net.agkn.hll.serialization.ISchemaVersion#schemaVersionNumber()
     */
    @Override public int schemaVersionNumber() {
        return SCHEMA_VERSION;
    }

    // ========================================================================
    // Type/Ordinal lookups
    /**
     * Gets the ordinal for the specified {@link HLLType}.
     *
     * @param  type the type whose ordinal is desired
     * @return the ordinal for the specified type, to be used in the version byte.
     *         This will always be non-negative.
     */
    private static int getOrdinal(final HLLType type) {
        for (int i = 0; i < TYPE_ORDINALS.length; i++)
            if (TYPE_ORDINALS[i].equals(type)) return i;

        throw new RuntimeException("Unknown HLL type " + type);
    }

    /**
     * Gets the {@link HLLType} for the specified ordinal.
     *
     * @param  ordinal the ordinal whose type is desired
     * @return the type for the specified ordinal. This will never be <code>null</code>.
     */
    private static HLLType getType(final int ordinal) {
        if ((ordinal < 0) || (ordinal >= TYPE_ORDINALS.length))
            throw new IllegalArgumentException("Invalid type ordinal '" + ordinal + "'. Only 0-" +
                (TYPE_ORDINALS.length - 1) + " inclusive allowed.");

        return TYPE_ORDINALS[ordinal];
    }
}
