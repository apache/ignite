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

package org.apache.ignite.internal.processors.cache.objects;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.processors.cache.transform.AbstractCacheObjectsTransformationTest;
import org.apache.ignite.spi.transform.CacheObjectTransformerSpi;
import org.apache.ignite.spi.transform.CacheObjectTransformerSpiAdapter;
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
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        CompressionTransformerSpi.zstdCnt.set(0);
        CompressionTransformerSpi.lz4Cnt.set(0);
        CompressionTransformerSpi.snapCnt.set(0);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheObjectTransformerSpi(new CompressionTransformerSpi());
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
    protected static class CompressionTransformerSpi extends CacheObjectTransformerSpiAdapter {
        /** Comptession type. */
        protected static volatile CompressionType type = CompressionType.defaultType();

        /** */
        private static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();

        /** */
        static final LZ4FastDecompressor lz4Decompressor = lz4Factory.fastDecompressor();

        /** */
        static final LZ4Compressor lz4Compressor = lz4Factory.highCompressor(1);

        /** Zstd restore count. */
        static final AtomicLong zstdCnt = new AtomicLong();

        /** Lz4 restore count. */
        static final AtomicLong lz4Cnt = new AtomicLong();

        /** Snappy restore count. */
        static final AtomicLong snapCnt = new AtomicLong();

        /** {@inheritDoc} */
        @Override protected boolean direct() {
            return true;
        }

        /** {@inheritDoc} */
        @Override protected ByteBuffer transform(ByteBuffer original) throws IgniteCheckedException {
            if (type == CompressionType.DISABLED)
                throw new IgniteCheckedException("Disabled.");

            int locOverhead = 4; // Compression type, Integer.

            int lim = original.remaining() - (CacheObjectTransformerSpi.OVERHEAD + locOverhead);

            if (lim <= 0)
                throw new IgniteCheckedException("Compression is not profitable.");

            ByteBuffer compressed = byteBuffer(original.remaining() + locOverhead); // Same as original (SNAPPY requirement) + header.

            compressed.position(locOverhead); // Reserving for compression type.

            switch (type) {
                case ZSTD:
                    try {
                        compressed.limit(lim); // Limiting to gain compression profit.

                        Zstd.compress(compressed, original, 1);

                        compressed.flip();
                    }
                    catch (ZstdException e) {
                        throw new IgniteCheckedException(e);
                    }

                    break;

                case LZ4:
                    try {
                        compressed.limit(lim); // Limiting to gain compression profit.

                        lz4Compressor.compress(original, compressed);

                        compressed.flip();
                    }
                    catch (LZ4Exception e) {
                        throw new IgniteCheckedException(e);
                    }

                    break;

                case SNAPPY:
                    try {
                        log.info("Transforming [orig=" + original + ", comp=" + compressed); // TODO
                        ((IgniteLoggerEx)log).flush();

                        int size = Snappy.compress(original, compressed);

                        log.info("Transformed [size=" + size); // TODO
                        ((IgniteLoggerEx)log).flush();

                        if (size > lim) // Limiting to gain compression profit (ByteBuffer limit is ignoring by Snappy).
                            throw new IgniteCheckedException("Compression gains no profit.");

                        compressed.position(0);
                    }
                    catch (IOException | IllegalArgumentException e) {
                        throw new IgniteCheckedException(e);
                    }

                    break;

                default:
                    throw new UnsupportedOperationException();
            }

            compressed.putInt(type.ordinal());
            compressed.rewind();

            return compressed;
        }

        /** {@inheritDoc} */
        @Override protected ByteBuffer restore(ByteBuffer transformed, int length) {
            ByteBuffer restored = byteBuffer(length);

            switch (CompressionType.values()[transformed.getInt()]) {
                case ZSTD:
                    Zstd.decompress(restored, transformed);

                    restored.flip();

                    zstdCnt.incrementAndGet();

                    break;

                case LZ4:
                    lz4Decompressor.decompress(transformed, restored);

                    restored.flip();

                    lz4Cnt.incrementAndGet();

                    break;

                case SNAPPY:
                    try {
                        log.info("Restoring [trans=" + transformed + ", rest=" + restored); // TODO
                        ((IgniteLoggerEx)log).flush();

                        Snappy.uncompress(transformed, restored);

                        log.info("Restored"); // TODO
                        ((IgniteLoggerEx)log).flush();

                        snapCnt.incrementAndGet();
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
