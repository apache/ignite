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

package org.apache.ignite.internal.managers.communication;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.NonMarshallableMessage;

/**
 * Internal message used when transmitting fields annotated with @Compress over the network.
 * <p>
 * WARNING: CompressedMessage is not intended for explicit use in messages.
 */
public class CompressedMessage implements NonMarshallableMessage {
    /** Chunk size. */
    static final int CHUNK_SIZE = 10 * 1024;

    /** Reader buffer capacity. */
    static final int BUFFER_CAPACITY = 10 * CHUNK_SIZE;

    /** Temporary buffer for compressed data received over the network. */
    ByteBuffer tmpBuf;

    /** Raw data size. */
    int dataSize;

    /** Chunked byte reader. */
    ChunkedByteReader chunkedReader;

    /** Chunk. */
    byte[] chunk;

    /** Flag indicating whether this is the last chunk. */
    boolean finalChunk;

    /** Compression level. */
    private int compressionLvl;

    /** Constructor. */
    public CompressedMessage() {
        // No-op.
    }

    /**
     * @param buf Source buffer with serialized data.
     * @param compressionLvl Compression level.
     */
    public CompressedMessage(ByteBuffer buf, int compressionLvl) {
        dataSize = buf.remaining();
        this.compressionLvl = compressionLvl;

        if (dataSize > 0)
            chunkedReader = new ChunkedByteReader(compress(buf));
    }

    /** @return Raw data size. */
    public int dataSize() {
        return dataSize;
    }

    /** @return Uncompressed data. */
    public byte[] uncompressed() {
        assert finalChunk;

        return uncompress();
    }

    /** @return Next chunk of data or null. */
    public byte[] nextChunk() {
        return chunkedReader.nextChunk();
    }

    /**
     * @param buf Buffer.
     * @return Compressed data.
     */
    private byte[] compress(ByteBuffer buf) {
        byte[] data = new byte[dataSize];

        buf.get(data);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
        Deflater deflater = new Deflater(compressionLvl, true);

        try (DeflaterOutputStream dos = new DeflaterOutputStream(baos, deflater)) {
            dos.write(data);
            dos.finish();
        }
        catch (IOException ex) {
            throw new IgniteException(ex);
        }
        finally {
            deflater.end();
        }

        return baos.toByteArray();
    }

    /** @return Uncompressed data. */
    private byte[] uncompress() {
        if (tmpBuf == null)
            return null;

        byte[] uncompressedData;

        Inflater inflater = new Inflater(true);

        byte[] bytes = new byte[tmpBuf.position()];

        tmpBuf.flip();
        tmpBuf.get(bytes);

        try (InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(bytes), inflater)) {
            uncompressedData = iis.readAllBytes();
        }
        catch (IOException ex) {
            throw new IgniteException(ex);
        }
        finally {
            inflater.end();
        }

        assert uncompressedData != null;
        assert uncompressedData.length == dataSize : "Expected=" + dataSize + ", actual=" + uncompressedData.length;

        tmpBuf = null;

        return uncompressedData;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CompressedMessage.class, this);
    }

    /** Byte reader returning data chunks of predefined size. */
    private static class ChunkedByteReader {
        /** Input data. */
        private final byte[] inputData;

        /** Current position. */
        private int pos;

        /** Constructor. */
        ChunkedByteReader(byte[] inputData) {
            this.inputData = inputData;
        }

        /** @return Next chunk of bytes or null. */
        byte[] nextChunk() {
            if (pos >= inputData.length)
                return null;

            int curChunkSize = Math.min(inputData.length - pos, CHUNK_SIZE);

            byte[] chunk = new byte[curChunkSize];

            System.arraycopy(inputData, pos, chunk, 0, curChunkSize);

            pos += curChunkSize;

            return chunk;
        }
    }
}
