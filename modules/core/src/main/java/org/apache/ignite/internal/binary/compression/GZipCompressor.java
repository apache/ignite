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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of {@link Compressor} which use GZIP compression library for compressing data.
 *
 * @see GZIPInputStream
 * @see GZIPOutputStream
 */
public class GZipCompressor implements Compressor {
    /**
     * {@inheritDoc}
     *
     * @see GZIPOutputStream
     */
    @Override public byte[] compress(@NotNull byte[] bytes) throws IOException {
        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStream out = new GZIPOutputStream(baos);
        ) {
            out.write(bytes);
            out.close();// need it, otherwise EOFException at decompressing
            return baos.toByteArray();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see GZIPInputStream
     */
    @Override public byte[] decompress(@NotNull byte[] bytes) throws IOException {
        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            InputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes));
        ) {
            byte[] buffer = new byte[32];
            int length;
            while ((length = in.read(buffer)) != -1)
                baos.write(buffer, 0, length);
            baos.flush();
            return baos.toByteArray();
        }
    }
}
