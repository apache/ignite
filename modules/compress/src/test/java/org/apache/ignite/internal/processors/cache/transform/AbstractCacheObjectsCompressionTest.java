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

package org.apache.ignite.internal.processors.cache.transform;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.CacheObjectTransformerUtils;
import org.apache.ignite.spi.transform.CacheObjectTransformerAdapter;
import org.xerial.snappy.Snappy;

/**
 *
 */
public abstract class AbstractCacheObjectsCompressionTest extends AbstractCacheObjectsTransformationTest {
    /** Huge string. */
    protected static final String HUGE_STRING;

    static {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 1000; i++)
            sb.append("A");

        HUGE_STRING = sb.toString();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheObjectTransformer(new CompressionTransformer());
    }

    /**
     *
     */
    protected static final class StringData {
        /** S. */
        private final String s;

        /**
         * @param s S.
         */
        public StringData(String s) {
            this.s = s;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            StringData data = (StringData)o;

            return Objects.equals(s, data.s);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(s);
        }
    }

    /**
     *
     */
    protected static class CompressionTransformer extends CacheObjectTransformerAdapter {
        /** Comptession type. */
        protected static volatile CompressionType type = CompressionType.defaultType();

        /** */
        private static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();

        /** */
        static final LZ4FastDecompressor lz4Decompressor = lz4Factory.fastDecompressor();

        /** */
        static final LZ4Compressor lz4Compressor = lz4Factory.highCompressor(1);

        /** {@inheritDoc} */
        @Override public boolean direct() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer transform(ByteBuffer original) {
            if (type == CompressionType.DISABLED)
                return null;

            int locOverhead = 8; // Compression type + length.
            int totalOverhead = CacheObjectTransformerUtils.OVERHEAD + locOverhead;

            int origSize = original.remaining();
            int lim = origSize - totalOverhead;

            if (lim <= 0)
                return null;

            int maxCompLen;

            switch (type) {
                case ZSTD:
                    maxCompLen = (int)Zstd.compressBound(origSize);

                    break;

                case LZ4:
                    maxCompLen = lz4Compressor.maxCompressedLength(origSize);

                    break;

                case SNAPPY:
                    maxCompLen = Snappy.maxCompressedLength(origSize);

                    break;

                default:
                    throw new UnsupportedOperationException();
            }

            ByteBuffer compressed = byteBuffer(locOverhead + maxCompLen);

            compressed.position(locOverhead);

            int size;

            switch (type) {
                case ZSTD:
                    size = Zstd.compress(compressed, original, 1);

                    compressed.flip();

                    break;

                case LZ4:
                    lz4Compressor.compress(original, compressed);

                    size = compressed.position() - locOverhead;

                    compressed.flip();

                    break;

                case SNAPPY:
                    try {
                        size = Snappy.compress(original, compressed);

                        compressed.position(0);
                    }
                    catch (IOException e) {
                        return null;
                    }

                    break;

                default:
                    throw new UnsupportedOperationException();
            }

            compressed.putInt(type.ordinal());
            compressed.putInt(origSize);

            assertEquals(locOverhead, compressed.position());

            compressed.rewind();

            if (size >= lim) // Limiting to gain compression profit.
                return null;

            return compressed;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer restore(ByteBuffer transformed) {
            CompressionType type = CompressionType.values()[transformed.getInt()];
            int length = transformed.getInt();

            ByteBuffer restored = byteBuffer(length);

            switch (type) {
                case ZSTD:
                    Zstd.decompress(restored, transformed);

                    restored.flip();

                    break;

                case LZ4:
                    lz4Decompressor.decompress(transformed, restored);

                    restored.flip();

                    break;

                case SNAPPY:
                    try {
                        Snappy.uncompress(transformed, restored);
                    }
                    catch (IOException e) {
                        throw new IgniteException(e);
                    }

                    break;

                default:
                    throw new UnsupportedOperationException();
            }

            return restored;
        }

        /**
         *
         */
        protected enum CompressionType {
            /** Compression disabled. */
            DISABLED,

            /** Zstd compression. */
            ZSTD,

            /** LZ4 compression. */
            LZ4,

            /** Snappy compression. */
            SNAPPY;

            /**
             * @return default.
             */
            static CompressionType defaultType() {
                return ZSTD;
            }
        }
    }
}
