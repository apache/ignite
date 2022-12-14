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
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.transform.AbstractCacheObjectsTransformationTest;
import org.apache.ignite.spi.transform.CacheObjectsTransformer;
import org.xerial.snappy.Snappy;

/**
 *
 */
public abstract class AbstractCacheObjectsCompressionTest extends AbstractCacheObjectsTransformationTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheObjectsTransformSpi(new CacheObjectsTransformSpiAdapter() {
                @Override public CacheObjectsTransformer transformer(CacheConfiguration<?, ?> ccfg) {
                    return new CompressionTransformer();
                }
            });
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
    protected static class CompressionTransformer implements CacheObjectsTransformer {
        /** Comptession type. */
        protected static CompressionType type = CompressionType.defaultType();

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
        @Override public int transform(ByteBuffer original, ByteBuffer compressed, int overhead) throws IgniteCheckedException {
            if (type == CompressionType.DISABLED)
                throw new IgniteCheckedException("Disabled.");

            if (compressed.capacity() < original.remaining())
                return original.remaining();

            int locOverhead = 4;

            int lim = original.remaining() - overhead - locOverhead;

            if (lim <= 0)
                throw new IgniteCheckedException("Compression is not possible.");

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
                        int size = Snappy.compress(original, compressed);

                        if (size > lim) // Limiting to gain compression profit.
                            throw new IgniteCheckedException("Compression gains no profit.");

                        compressed.position(0);
                    }
                    catch (IOException e) {
                        throw new IgniteCheckedException(e);
                    }

                    break;

                default:
                    throw new UnsupportedOperationException();
            }

            compressed.putInt(type.ordinal());
            compressed.position(0);

            return 0;
        }

        /** {@inheritDoc} */
        @Override public void restore(ByteBuffer compressed, ByteBuffer restored) {
            switch (CompressionType.values()[compressed.getInt()]) {
                case ZSTD:
                    Zstd.decompress(restored, compressed);

                    restored.flip();

                    break;

                case LZ4:
                    lz4Decompressor.decompress(compressed, restored);

                    restored.flip();

                    break;

                case SNAPPY:
                    try {
                        Snappy.uncompress(compressed, restored);
                    }
                    catch (IOException e) {
                        throw new IgniteException(e);
                    }

                    break;

                default:
                    throw new UnsupportedOperationException();
            }
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
             * @return
             */
            static CompressionType defaultType() {
                return ZSTD;
            }
        }
    }
}
