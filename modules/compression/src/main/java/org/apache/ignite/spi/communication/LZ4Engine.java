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
    private static final int BLOCK_LENGTH = 4;

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

        try {
            int size = compressor.compress(src, src.position(), src.remaining(),
                buf, buf.position() + BLOCK_LENGTH, buf.remaining() - BLOCK_LENGTH);

            buf.putInt(size);

            buf.position(buf.position() + size);
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

        if (src.remaining() < BLOCK_LENGTH)
            return BUFFER_UNDERFLOW;

        int initPos = src.position();

        int compressedLen = src.getInt();

        assert compressedLen > 0;

        if (src.remaining() < compressedLen) {
            src.position(initPos);

            return BUFFER_UNDERFLOW;
        }

        try {
            int size = decompressor.decompress(src, src.position(), compressedLen,
                buf, buf.position(), buf.remaining());

            buf.position(buf.position() + size);
            src.position(src.position() + compressedLen);
        }
        catch (LZ4Exception e) {
            src.position(initPos);

            return BUFFER_OVERFLOW;
        }

        return OK;
    }
}
