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
 * A serialization schema for HLLs. Reads and writes HLL metadata to
 * and from <code>byte[]</code> representations.
 *
 * @author timon
 */
public interface ISchemaVersion {
    /**
     * The number of metadata bytes required for a serialized HLL of the
     * specified type.
     *
     * @param  type the type of the serialized HLL
     * @return the number of padding bytes needed in order to fully accommodate
     *         the needed metadata.
     */
    int paddingBytes(HLLType type);

    /**
     * Writes metadata bytes to serialized HLL.
     *
     * @param bytes the padded data bytes of the HLL
     * @param metadata the metadata to write to the padding bytes
     */
    void writeMetadata(byte[] bytes, IHLLMetadata metadata);

    /**
     * Reads the metadata bytes of the serialized HLL.
     *
     * @param  bytes the serialized HLL
     * @return the HLL metadata
     */
    IHLLMetadata readMetadata(byte[] bytes);

    /**
     * Builds an HLL serializer that matches this schema version.
     *
     * @param  type the HLL type that will be serialized. This cannot be
     *         <code>null</code>.
     * @param  wordLength the length of the 'words' that comprise the data of the
     *         HLL. Words must be at least 5 bits and at most 64 bits long.
     * @param  wordCount the number of 'words' in the HLL's data.
     * @return a byte array serializer used to serialize a HLL according
     *         to this schema version's specification.
     * @see #paddingBytes(HLLType)
     * @see IWordSerializer
     */
    IWordSerializer getSerializer(HLLType type, int wordLength, int wordCount);

    /**
     * Builds an HLL deserializer that matches this schema version.
     *
     * @param  type the HLL type that will be deserialized. This cannot be
     *         <code>null</code>.
     * @param  wordLength the length of the 'words' that comprise the data of the
     *         serialized HLL. Words must be at least 5 bits and at most 64
     *         bits long.
     * @param  bytes the serialized HLL to deserialize. This cannot be
     *         <code>null</code>.
     * @return a byte array deserializer used to deserialize a HLL serialized
     *         according to this schema version's specification.
     */
    IWordDeserializer getDeserializer(HLLType type, int wordLength, byte[] bytes);

    /**
     * @return the schema version number.
     */
    int schemaVersionNumber();
}
