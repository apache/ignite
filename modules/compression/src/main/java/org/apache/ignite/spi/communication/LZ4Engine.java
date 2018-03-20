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

import java.io.IOException;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.ignite.internal.util.nio.compression.CompressionEngine;
import org.apache.ignite.internal.util.nio.compression.CompressionEngineResult;

import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.BUFFER_UNDERFLOW;
import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.OK;

/**
 * Implementation of LZ4 algorithm.
 */
public final class LZ4Engine implements CompressionEngine {
    /** */
    private final LZ4Compressor compressor;

    /** */
    private final LZ4SafeDecompressor decompressor;

    /** */
    public LZ4Engine() {
        LZ4Factory factory = LZ4Factory.fastestInstance();

        compressor = factory.fastCompressor();
        decompressor = factory.safeDecompressor();
    }

    /** {@inheritDoc} */
    @Override public CompressionEngineResult compress(ByteBuffer src, ByteBuffer buf) {
        assert src != null;
        assert buf != null;
        assert buf.position() + 4 /* Block length */ <= Integer.MAX_VALUE;

        try {
            int compress = compressor.compress(src, src.position(), src.remaining(),
                buf, buf.position() + 4, buf.remaining() - 4);

            putInt(compress, buf);

            buf.position(buf.position() + compress);
            src.position(src.position() + src.remaining());
        }
        catch (LZ4Exception e) {
            return BUFFER_OVERFLOW;
        }

        return OK;
    }

    /** {@inheritDoc} */
    @Override public CompressionEngineResult decompress(ByteBuffer src, ByteBuffer buf) {
        assert src != null;
        assert buf != null;

        int len = src.remaining();
        int initPos = src.position();

        if (len < 4 /* Block length */)
            return BUFFER_UNDERFLOW;

        int compressedLen = getInt(src);

        assert compressedLen > 0;

        if (src.remaining() < compressedLen) {
            src.position(initPos);

            return BUFFER_UNDERFLOW;
        }

        try {
            int decompress = decompressor.decompress(src, src.position(), compressedLen,
                buf, buf.position(), buf.remaining());

            buf.position(buf.position() + decompress);
            src.position(src.position() + compressedLen);
        }
        catch (LZ4Exception e) {
            src.position(initPos);

            return BUFFER_OVERFLOW;
        }

        return OK;
    }

    /**
     * Read {@code int} value from a byte buffer disregard byte order.
     *
     * @param buf ByteBuffer.
     * @return {@code int} Value.
     */
    private static int getInt(ByteBuffer buf) {
        assert buf.remaining() >=4;

        return ((buf.get() & 0xFF) << 24) | ((buf.get() & 0xFF) << 16)
            | ((buf.get() & 0xFF) << 8) | (buf.get() & 0xFF);
    }

    /**
     * Write {@code int} value to a byte buffer disregard byte order.
     *
     * @param int Value.
     * @param buf ByteBuffer.
     */
    private static void putInt(int val, ByteBuffer buf) {
        assert buf.remaining() >=4;

        buf.put((byte)(val >>> 24));
        buf.put((byte)(val >>> 16));
        buf.put((byte)(val >>> 8));
        buf.put((byte)(val));
    }
}
