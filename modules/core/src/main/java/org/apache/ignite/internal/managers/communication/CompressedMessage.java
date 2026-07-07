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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Internal message used when transmitting fields annotated with @Compress over the network.
 * <p>
 * WARNING: CompressedMessage is not intended for explicit use in messages.
 */
public class CompressedMessage implements Message {
    /** Chunk size. */
    static final int CHUNK_SIZE = 10 * 1024;

    /** Compressed data chunks: filled by {@link #compress(ByteBuffer)} on send, by the serializer on receive. */
    List<byte[]> chunks;

    /** Index of the next chunk to send. */
    private int chunkIdx;

    /** Raw data size. */
    int dataSize;

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
            compress(buf);
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
        return chunkIdx < chunks.size() ? chunks.get(chunkIdx++) : null;
    }

    /** @param buf Buffer. */
    private void compress(ByteBuffer buf) {
        Deflater deflater = new Deflater(compressionLvl, true);

        try {
            deflater.setInput(buf);
            deflater.finish();

            chunks = new ArrayList<>(dataSize / CHUNK_SIZE + 1);

            // Incompressible data may expand: raw-deflate worst case below CHUNK_SIZE is one stored block (~11 bytes overhead).
            byte[] chunk0 = new byte[dataSize >= CHUNK_SIZE ? CHUNK_SIZE : dataSize + 16];

            int len = 0;

            while (!deflater.finished()) {
                len += deflater.deflate(chunk0, len, chunk0.length - len);

                if (len == chunk0.length && !deflater.finished()) {
                    chunks.add(chunk0);

                    chunk0 = new byte[CHUNK_SIZE];
                    len = 0;
                }
            }

            if (len > 0)
                chunks.add(len == chunk0.length ? chunk0 : Arrays.copyOf(chunk0, len));
        }
        finally {
            deflater.end();
        }
    }

    /** @return Uncompressed data. */
    private byte[] uncompress() {
        if (chunks == null)
            throw new IgniteException("Compressed stream is truncated [expected=" + dataSize + ", inflated=0]");

        long compressedTotal = 0;

        for (int i = 0; i < chunks.size(); i++)
            compressedTotal += chunks.get(i).length;

        // Raw deflate cannot expand beyond ~1032:1, a larger claim means a corrupted size header; also bounds the
        // upfront allocation by ~1032x of the actually received bytes.
        if (dataSize > compressedTotal * 1032L + 64)
            throw new IgniteException("Invalid compressed message data size [dataSize=" + dataSize +
                ", compressedBytes=" + compressedTotal + ']');

        byte[] data = new byte[dataSize];

        Inflater inflater = new Inflater(true);

        try {
            int off = 0;
            int i = 0;

            for (; i < chunks.size() && off < dataSize; i++) {
                inflater.setInput(chunks.get(i));

                int n;

                while (off < dataSize && (n = inflater.inflate(data, off, dataSize - off)) > 0)
                    off += n;
            }

            if (off != dataSize)
                throw new IgniteException("Compressed stream is truncated [expected=" + dataSize + ", inflated=" + off + ']');

            // Any extra inflatable byte means the size header is understated.
            byte[] probe = new byte[1];

            while (true) {
                if (inflater.inflate(probe, 0, 1) > 0)
                    throw new IgniteException("Compressed stream is longer than expected [expected=" + dataSize + ']');

                if (inflater.finished() || i == chunks.size())
                    break;

                inflater.setInput(chunks.get(i++));
            }
        }
        catch (DataFormatException e) {
            throw new IgniteException(e);
        }
        finally {
            inflater.end();
        }

        chunks = null;

        return data;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CompressedMessage.class, this);
    }
}
