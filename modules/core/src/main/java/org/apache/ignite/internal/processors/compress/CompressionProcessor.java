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

package org.apache.ignite.internal.processors.compress;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.PageCompression;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 * Compression processor.
 */
public class CompressionProcessor extends GridProcessorAdapter {
    /** */
    public static final byte UNCOMPRESSED_PAGE = 0;

    /** */
    public static final byte COMPACTED_PAGE = 1;

    /** */
    public static final byte ZSTD_COMPRESSED_PAGE = 2;

    /** */
    public static final byte LZ4_COMPRESSED_PAGE = 3;

    /**
     * @param ctx Kernal context.
     */
    public CompressionProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param compression Compression algorithm.
     * @return Default compression level.
     */
    public static int getDefaultCompressionLevel(PageCompression compression) {
        switch (compression) {
            case ZSTD:
                return 3;

            case LZ4:
                return 0;

            case DROP_GARBAGE:
                return 0;
        }

        throw new IllegalArgumentException("Compression: " + compression);
    }

    /**
     * @param compressLevel Compression level.
     * @param compression Compression algorithm.
     * @return Compression level.
     */
    public static int checkCompressionLevelBounds(int compressLevel, PageCompression compression) {
        switch (compression) {
            case ZSTD:
                // TODO Use Zstd.minCompressionLevel() and Zstd.maxCompressionLevel() after zstd jar upgrade.
                if (compressLevel < -131072 || compressLevel > 22)
                    throw new IllegalArgumentException("Compression level for ZSTD must be between -131072 and 22." );
                break;

            case LZ4:
                if (compressLevel < 0 || compressLevel > 17)
                    throw new IllegalArgumentException("Compression level for LZ4 must be between 0 and 17." );
                break;

            case DROP_GARBAGE:
                break;

            default:
                throw new IllegalArgumentException("Compression: " + compression);
        }

        return compressLevel;
    }

    /**
     * @param page Page buffer.
     * @param storeBlockSize Store block size.
     * @param compression Compression algorithm.
     * @param compressLevel Compression level.
     * @return Possibly compressed buffer.
     * @throws IgniteCheckedException If failed.
     */
    public ByteBuffer compressPage(
        ByteBuffer page,
        int storeBlockSize,
        PageCompression compression,
        int compressLevel
    ) throws IgniteCheckedException {
        throw new IgniteCheckedException("Page compression failed: make sure that ignite-compression module is in classpath.");
    }

    /**
     * @param page Possibly compressed page buffer.
     * @throws IgniteCheckedException If failed.
     */
    public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        if (PageIO.getCompressionType(page) != UNCOMPRESSED_PAGE)
            throw new IgniteCheckedException("Page decompression failed: make sure that ignite-compression module is in classpath.");
    }
}
