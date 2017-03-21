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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.jetbrains.annotations.NotNull;

/**
 * TODO: description
 */
public class DeflaterCompressor implements Compressor {

    /** Compression processor */
    private Deflater compressor;

    /** Decompression processor */
    private Inflater decompressor;

    /** Default constructor */
    public DeflaterCompressor() {
        this.compressor = new Deflater();
        this.decompressor = new Inflater();
    }

    /**
     * @param level - Compression level
     */
    public DeflaterCompressor(int level) {
        if (level < Deflater.DEFAULT_COMPRESSION || level > Deflater.BEST_COMPRESSION)
            throw new IllegalArgumentException("Invalid compression level: " + level);

        this.compressor = new Deflater(level);
        this.decompressor = new Inflater();
    }

    /** {@inheritDoc} */
    @Override public byte[] compress(@NotNull byte[] bytes) throws IOException {
        compressor.setInput(bytes);
        compressor.finish();

        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream()
        ) {
            byte[] buffer = new byte[100];

            while (!compressor.finished()) {
                int length = compressor.deflate(buffer);
                baos.write(buffer, 0, length);
            }
            return baos.toByteArray();
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] decompress(@NotNull byte[] bytes) throws IOException {
        decompressor.setInput(bytes);

        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream()
        ) {
            byte[] buffer = new byte[100];

            while (!decompressor.finished()) {
                int length = decompressor.inflate(buffer);
                baos.write(buffer, 0, length);
            }
            return baos.toByteArray();
        }
        catch (DataFormatException ignored) {
            return new byte[0];
        }
    }
}
