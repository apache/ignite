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
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 * Compression processor.
 *
 * @see IgniteComponentType#COMPRESSION
 */
public class CompressionProcessor extends GridProcessorAdapter {
    /** */
    public static final int LZ4_MIN_LEVEL = 0;

    /** */
    public static final int LZ4_MAX_LEVEL = 17;

    /** */
    public static final int LZ4_DEFAULT_LEVEL = 0;

    /** */
    public static final int ZSTD_MIN_LEVEL = -131072;

    /** */
    public static final int ZSTD_MAX_LEVEL = 22;

    /** */
    public static final int ZSTD_DEFAULT_LEVEL = 3;

    /** */
    protected static final byte UNCOMPRESSED_PAGE = 0;

    /** */
    protected static final byte COMPACTED_PAGE = 1;

    /** */
    protected static final byte ZSTD_COMPRESSED_PAGE = 2;

    /** */
    protected static final byte LZ4_COMPRESSED_PAGE = 3;

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
                return ZSTD_DEFAULT_LEVEL;

            case LZ4:
                return LZ4_DEFAULT_LEVEL;

            case SKIP_GARBAGE:
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
                checkCompressionLevelBounds(compressLevel, ZSTD_MIN_LEVEL, ZSTD_MAX_LEVEL);
                break;

            case LZ4:
                checkCompressionLevelBounds(compressLevel, LZ4_MIN_LEVEL, LZ4_MAX_LEVEL);
                break;

            case SKIP_GARBAGE:
                break;

            default:
                throw new IllegalArgumentException("Compression: " + compression);
        }
        return compressLevel;
    }

    /**
     * @param compressLevel Compression level.
     * @param min Min level.
     * @param max Max level.
     */
    private static void checkCompressionLevelBounds(int compressLevel, int min, int max) {
        if (compressLevel < min  || compressLevel > max) {
            throw new IllegalArgumentException("Compression level for LZ4 must be between " + min +
                " and " + max + ".");
        }
    }

    /**
     * @throws IgniteCheckedException Always.
     */
    private <T> T fail() throws IgniteCheckedException {
        throw new IgniteCheckedException("Make sure that ignite-compression module is in classpath.");
    }

    /**
     * @throws IgniteCheckedException If compression is not supported.
     */
    public void checkPageCompressionSupported() throws IgniteCheckedException {
        fail();
    }

    /**
     * @param page Page buffer.
     * @param pageSize Page size.
     * @param storeBlockSize Store block size.
     * @param compression Compression algorithm.
     * @param compressLevel Compression level.
     * @return Possibly compressed buffer.
     * @throws IgniteCheckedException If failed.
     */
    public ByteBuffer compressPage(
        ByteBuffer page,
        int pageSize,
        int storeBlockSize,
        PageCompression compression,
        int compressLevel
    ) throws IgniteCheckedException {
        return fail();
    }

    /**
     * @param page Possibly compressed page buffer.
     * @param pageSize Page size.
     * @throws IgniteCheckedException If failed.
     */
    public void decompressPage(ByteBuffer page, int pageSize) throws IgniteCheckedException {
        if (PageIO.getCompressionType(page) != UNCOMPRESSED_PAGE)
            fail();
    }
}
