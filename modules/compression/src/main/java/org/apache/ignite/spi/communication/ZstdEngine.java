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

package org.apache.ignite.spi.communication;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.nio.compression.CompressionEngine;
import org.apache.ignite.internal.util.nio.compression.CompressionEngineResult;

import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.BUFFER_UNDERFLOW;
import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.OK;

/**
 * Factory for compression engine with Zstd algorithm.
 */
public final class ZstdEngine implements CompressionEngine {
    /** */
    private static final int COMPRESS_LEVEL = 1;

    /** */
    private static final int ZSTD_ERROR_DST_SIZE_TOO_SMALL = -70;

    /** */
    private static final int INIT_ARR_SIZE = 1 << 15;

    /** */
    private byte[] compressArr = new byte[INIT_ARR_SIZE];

    /** */
    private byte[] decompressArr = new byte[INIT_ARR_SIZE];

    /** {@inheritDoc} */
    @Override public CompressionEngineResult compress(ByteBuffer src, ByteBuffer buf) throws IOException {
        assert src != null;
        assert buf != null;

        if (src.isDirect() && buf.isDirect()) {
            long size = Zstd.compressDirectByteBuffer(buf, buf.position(),buf.limit() - buf.position(),
                src, src.position(),src.limit() - src.position(), COMPRESS_LEVEL);

            if (Zstd.isError(size)) {
                if (size == ZSTD_ERROR_DST_SIZE_TOO_SMALL)
                    return BUFFER_OVERFLOW;

                throw new IOException("Failed to compress data: " + Zstd.getErrorName(size));
            }

            src.position(src.limit());
            buf.position(buf.position() + (int) size);

            return OK;
        }
        else {
            byte[] inputArr = new byte[src.remaining()];

            src.get(inputArr);

            long res;

            do {
                res = Zstd.compress(compressArr, inputArr, COMPRESS_LEVEL);

                if (Zstd.isError(res)) {
                    if (res == ZSTD_ERROR_DST_SIZE_TOO_SMALL) {
                        assert decompressArr.length <= Integer.MAX_VALUE / 2;

                        compressArr = new byte[compressArr.length * 2];
                    }
                    else
                        throw new IOException("Failed to compress data: " + Zstd.getErrorName(res));
                }
            }
            while (res == ZSTD_ERROR_DST_SIZE_TOO_SMALL);

            if (res > buf.remaining()) {
                src.rewind();

                return BUFFER_OVERFLOW;
            }

            buf.put(compressArr, 0, (int)res);

            return OK;
        }
    }

    /** {@inheritDoc} */
    @Override public CompressionEngineResult decompress(ByteBuffer src, ByteBuffer buf) throws IOException {
        assert src != null;
        assert buf != null;

        int frameSize = readFrameSize(src);

        if (frameSize == -1)
            return BUFFER_UNDERFLOW;

        if (src.isDirect() && buf.isDirect()) {
            int oldLimit = src.limit();

            src.limit(src.position() + frameSize);

            long size = Zstd.decompressDirectByteBuffer(buf, buf.position(), buf.limit() - buf.position(),
                src, src.position(),src.limit() - src.position());

            if (Zstd.isError(size)) {
                src.limit(oldLimit);

                if (size == ZSTD_ERROR_DST_SIZE_TOO_SMALL)
                    return BUFFER_OVERFLOW;

                throw new IOException("Failed to decompress data: " + Zstd.getErrorName(size));
            }

            src.position(src.limit());
            buf.position(buf.position() + (int)size);

            src.limit(oldLimit);

            return OK;
        }
        else {
            int initPos = src.position();

            byte[] inputWrapArr = new byte[frameSize];

            src.get(inputWrapArr);

            long res;

            do {
                res = Zstd.decompress(decompressArr, inputWrapArr);

                if (Zstd.isError(res)) {
                    if (res == ZSTD_ERROR_DST_SIZE_TOO_SMALL) {
                        assert decompressArr.length <= Integer.MAX_VALUE / 2;

                        decompressArr = new byte[decompressArr.length * 2];
                    }
                    else
                        throw new IOException("Failed to decompress data: " + Zstd.getErrorName(res));
                }
            }
            while (res == ZSTD_ERROR_DST_SIZE_TOO_SMALL);

            if (res > buf.remaining()) {
                src.position(initPos);

                return BUFFER_OVERFLOW;
            }

            buf.put(decompressArr, 0, (int)res);

            return OK;
        }
    }

    /**
     * Read size of a compressed frame.
     *
     * @param buf ByteBuffer.
     * @return size of compressed frame, -1 otherwise if not enough data.
     */
    private static int readFrameSize(ByteBuffer buf) throws IOException {
        int initPos = buf.position();
        int limit = buf.limit();

        if (initPos + 4 + 1 + 3 /* MAGIC + FRAME_HEADER + BLOCK_HEADER */ > limit)
            return -1;

        int off = initPos;

        boolean magicCheck = buf.get(off++) == 40 &&
                            buf.get(off++) == -75 &&
                            buf.get(off++) == 47 &&
                            buf.get(off++) == -3;

        if (!magicCheck)
            throw new IOException("Invalid magic prefix.");

        int frameHdrDesc = buf.get(off) & 0xFF;

        int contentSizeDesc = frameHdrDesc >>> 6;
        boolean singleSegment = (frameHdrDesc & 0b100000) != 0;
        boolean hasChecksum = (frameHdrDesc & 0b100) != 0;
        int dictionaryDesc = frameHdrDesc & 0b11;

        int hdrSize = 1 +
            (singleSegment ? 0 : 1) +
            (dictionaryDesc == 0 ? 0 : (1 << (dictionaryDesc - 1))) +
            (contentSizeDesc == 0 ? (singleSegment ? 1 : 0) : (1 << contentSizeDesc));

        if (off + hdrSize > limit)
            return -1;

        off += hdrSize;

        boolean lastBlock;

        do {
            if (off + 3 > limit)
                return -1;

            // read block header
            int hdr = (
                ((buf.get(off + 2) & 0xff) << 16) |
                    ((buf.get(off + 1) & 0xff) <<  8) |
                    ((buf.get(off) & 0xff))) & 0xFF_FFFF;

            off += 3 /* SIZE_OF_BLOCK_HEADER */;

            lastBlock = (hdr & 1) != 0;
            int blockType = (hdr >>> 1) & 0b11;
            int blockSize = (hdr >>> 3) & 0b111111111111111111111; // 21 bits

            switch (blockType) {
                case 0:
                case 2:
                    off += blockSize;
                    break;
                case 1:
                    off += 1;
                    break;
            }
        }
        while (!lastBlock);

        if (hasChecksum)
            off += 3;

        if (off > limit)
            return -1;

        return off - initPos;
    }
}
