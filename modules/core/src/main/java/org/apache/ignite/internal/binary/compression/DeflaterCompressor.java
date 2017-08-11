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

package org.apache.ignite.internal.binary.compression;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import org.apache.ignite.binary.BinaryObjectException;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of {@link Compressor} which use ZLIB compression library for compressing data.
 *
 * @see Inflater
 * @see Deflater
 */
public class DeflaterCompressor implements Compressor {
    /**
     * {@inheritDoc}
     *
     * @see Deflater
     */
    @Override public byte[] compress(@NotNull byte[] bytes) throws BinaryObjectException {
        Deflater compressor = new Deflater();

        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DeflaterOutputStream deflaterStream = new DeflaterOutputStream(baos, compressor)
        ) {
            deflaterStream.write(bytes);
            deflaterStream.finish();

            return baos.toByteArray();
        } catch (Exception e) {
            throw new BinaryObjectException("Failed to compress bytes", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see Inflater
     */
    @Override public byte[] decompress(@NotNull byte[] bytes) throws BinaryObjectException {
        Inflater decompressor = new Inflater();
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
        catch (Exception e) {
            throw new BinaryObjectException("Failed to decompress bytes", e);
        }
    }
}
