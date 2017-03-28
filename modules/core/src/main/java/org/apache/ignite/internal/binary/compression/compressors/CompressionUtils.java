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

package org.apache.ignite.internal.binary.compression.compressors;

import java.io.IOException;
import java.util.Map;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryWriteMode;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.compression.CompressionType;
import org.jetbrains.annotations.NotNull;

/**
 * Utilities methods for working with compression.
 */
public class CompressionUtils {

    /**
     * Compresses given bytes, using {@link Compressor} which defined in BinaryContext
     * and mapped with given {@link CompressionType}.
     *
     * @param ctx BinaryContext.
     * @param type CompressionType.
     * @param bytes Non compressed bytes.
     * @return Compressed bytes.
     * @throws BinaryObjectException if failed to compress bytes.
     */
    public static byte[] compress(@NotNull BinaryContext ctx, @NotNull CompressionType type,
        @NotNull byte[] bytes) throws BinaryObjectException {
        Map<CompressionType, Compressor> compressorsSelector = ctx.configuration().getCompressorsSelector();

        return compress(compressorsSelector.get(type), bytes);
    }

    /**
     * Compresses given bytes, using given {@link Compressor}.
     *
     * @param compressor Compressor for compressing.
     * @param bytes Non compressed bytes.
     * @return Compressed bytes.
     * @throws BinaryObjectException if failed to compress bytes.
     */
    public static byte[] compress(@NotNull Compressor compressor, @NotNull byte[] bytes) throws BinaryObjectException {
        try {
            return compressor.compress(bytes);
        }
        catch (IOException e) {
            throw new BinaryObjectException("Failed to compress bytes", e);
        }
    }

    /**
     * Decompresses given bytes, using {@link Compressor} which defined in BinaryContext
     * and mapped with given {@link CompressionType}.
     *
     * @param ctx BinaryContext.
     * @param type CompressionType.
     * @param bytes Compressed bytes.
     * @return Decompressed bytes.
     * @throws BinaryObjectException if failed to decompress bytes.
     */
    public static byte[] decompress(@NotNull BinaryContext ctx, @NotNull CompressionType type,
        @NotNull byte[] bytes) throws BinaryObjectException {
        Map<CompressionType, Compressor> compressorsSelector = ctx.configuration().getCompressorsSelector();

        return decompress(compressorsSelector.get(type), bytes);
    }

    /**
     * Decompresses given bytes, using given {@link Compressor}.
     *
     * @param compressor Compressor for decompressing.
     * @param bytes Compressed bytes.
     * @return Decompressed bytes.
     * @throws BinaryObjectException if failed to decompress bytes.
     */
    public static byte[] decompress(@NotNull Compressor compressor,
        @NotNull byte[] bytes) throws BinaryObjectException {
        try {
            return compressor.decompress(bytes);
        }
        catch (IOException e) {
            throw new BinaryObjectException("Failed to decompress bytes", e);
        }
    }

    /**
     * Check if type is compressed.
     *
     * @param mode BinaryWriteMode.
     * @return 'true' if type is compressed, otherwise 'false'.
     */
    public static boolean isCompressionType(@NotNull BinaryWriteMode mode) {
        return isCompressionType(mode.typeId());
    }

    /**
     * Check if type is compressed.
     *
     * @param typeId Type id defined in the {@link GridBinaryMarshaller}.
     * @return 'true' if type is compressed, otherwise 'false'.
     */
    public static boolean isCompressionType(int typeId) {
        return (typeId == GridBinaryMarshaller.GZIPPED) ||
            (typeId == GridBinaryMarshaller.DEFLATED) ||
            (typeId == GridBinaryMarshaller.COMPRESSED_CUSTOM);
    }
}
