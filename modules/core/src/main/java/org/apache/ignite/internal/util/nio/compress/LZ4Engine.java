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

package org.apache.ignite.internal.util.nio.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.BUFFER_UNDERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.OK;

/**
 * Implementation of LZ4 algorithm.
 */
public class LZ4Engine implements CompressionEngine {
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
    public CompressionEngineResult wrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        try {
            int compress = compressor.compress(src, src.position(), src.remaining(), buf, buf.position() + 4, buf.remaining() - 4);

            putInt(compress, buf);

            buf.position(buf.position() + compress);
            src.position(src.position() + src.remaining());
        } catch (LZ4Exception e) {
            return BUFFER_OVERFLOW;
        }

        return OK;
    }

    /** {@inheritDoc} */
    public CompressionEngineResult unwrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        int len = src.remaining();
        int initPos = src.position();

        if (len < 5)
            return BUFFER_UNDERFLOW;

        int compressedLen = getInt(src);

        assert compressedLen >= 0;

        if (src.remaining() < compressedLen) {
            src.position(initPos);

            return BUFFER_UNDERFLOW;
        }

        try {
            int decompress = decompressor.decompress(src, src.position(), compressedLen, buf, buf.position(), buf.remaining());

            buf.position(buf.position() + decompress);
            src.position(src.position() + compressedLen);
        } catch (LZ4Exception e) {
            src.position(initPos);

            return BUFFER_OVERFLOW;
        }

        return OK;
    }

    /** */
    private int getInt(ByteBuffer buf){
        return ((buf.get() & 0xFF) << 24) | ((buf.get() & 0xFF) << 16)
            | ((buf.get() & 0xFF) << 8) | (buf.get() & 0xFF);
    }

    /** */
    private void putInt(int val, ByteBuffer buf){
        buf.put((byte)(val >>> 24));
        buf.put((byte)(val >>> 16));
        buf.put((byte)(val >>> 8));
        buf.put((byte)(val));
    }
}
