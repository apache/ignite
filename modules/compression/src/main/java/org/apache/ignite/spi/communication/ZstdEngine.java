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
    private static final int BLOCK_LENGTH = 4;

    /** */
    private byte[] compressArr = new byte[INIT_ARR_SIZE];

    /** */
    private byte[] decompressArr = new byte[INIT_ARR_SIZE];

    /** {@inheritDoc} */
    @Override public CompressionEngineResult compress(ByteBuffer src, ByteBuffer buf) throws IOException {
        assert src != null;
        assert buf != null;

        if (src.isDirect() && buf.isDirect()) {
            long res = Zstd.compressDirectByteBuffer(buf, buf.position() + BLOCK_LENGTH,
                buf.limit() - buf.position() - BLOCK_LENGTH, src, src.position(),
                src.limit() - src.position(), COMPRESS_LEVEL);

            if (Zstd.isError(res)) {
                if (res == ZSTD_ERROR_DST_SIZE_TOO_SMALL)
                    return BUFFER_OVERFLOW;

                throw new IOException("Failed to compress data: " + Zstd.getErrorName(res));
            }

            assert res <= Integer.MAX_VALUE;

            if (res + BLOCK_LENGTH > buf.remaining())
                return BUFFER_OVERFLOW;

            buf.putInt((int)res);

            src.position(src.limit());
            buf.position(buf.position() + (int)res);

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
                        assert compressArr.length <= Integer.MAX_VALUE / 2;

                        compressArr = new byte[compressArr.length * 2];
                    }
                    else
                        throw new IOException("Failed to compress data: " + Zstd.getErrorName(res));
                }
            }
            while (res == ZSTD_ERROR_DST_SIZE_TOO_SMALL);

            assert res <= Integer.MAX_VALUE;

            if (res + BLOCK_LENGTH > buf.remaining()) {
                src.rewind();

                return BUFFER_OVERFLOW;
            }

            buf.putInt((int)res);

            buf.put(compressArr, 0, (int)res);

            return OK;
        }
    }

    /** {@inheritDoc} */
    @Override public CompressionEngineResult decompress(ByteBuffer src, ByteBuffer buf) throws IOException {
        assert src != null;
        assert buf != null;

        if (src.remaining() < BLOCK_LENGTH)
            return BUFFER_UNDERFLOW;

        int initPos = src.position();

        int compressedLen = src.getInt();

        assert compressedLen > 0;

        if (src.remaining() < compressedLen) {
            src.position(initPos);

            return BUFFER_UNDERFLOW;
        }

        if (src.isDirect() && buf.isDirect()) {
            int oldLimit = src.limit();

            src.limit(src.position() + compressedLen);

            long res = Zstd.decompressDirectByteBuffer(buf, buf.position(), buf.limit() - buf.position(),
                src, src.position(), src.limit() - src.position());

            if (Zstd.isError(res)) {
                src.position(initPos);
                src.limit(oldLimit);

                if (res == ZSTD_ERROR_DST_SIZE_TOO_SMALL)
                    return BUFFER_OVERFLOW;

                throw new IOException("Failed to decompress data: " + Zstd.getErrorName(res));
            }

            src.position(src.limit());
            buf.position(buf.position() + (int)res);

            src.limit(oldLimit);

            return OK;
        }
        else {
            byte[] inputWrapArr = new byte[compressedLen];

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
}
