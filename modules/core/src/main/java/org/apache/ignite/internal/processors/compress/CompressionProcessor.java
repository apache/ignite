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
import java.nio.file.Path;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.ThreadLocalDirectByteBuffer;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CompactablePageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.configuration.DataStorageConfiguration.MAX_PAGE_SIZE;
import static org.apache.ignite.configuration.DiskPageCompression.SKIP_GARBAGE;
import static org.apache.ignite.internal.util.GridUnsafe.NATIVE_BYTE_ORDER;

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
    public static final byte UNCOMPRESSED_PAGE = 0;

    /** */
    protected static final byte COMPACTED_PAGE = 1;

    /** */
    protected static final byte ZSTD_COMPRESSED_PAGE = 2;

    /** */
    protected static final byte LZ4_COMPRESSED_PAGE = 3;

    /** */
    protected static final byte SNAPPY_COMPRESSED_PAGE = 4;

    /** Max page size. */
    private final ThreadLocalDirectByteBuffer compactBuf = new ThreadLocalDirectByteBuffer(MAX_PAGE_SIZE, NATIVE_BYTE_ORDER);

    /**
     * @param ctx Kernal context.
     */
    public CompressionProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param compressLevel Compression level.
     * @param compression Compression algorithm.
     * @return Compression level.
     */
    public static int getCompressionLevel(Integer compressLevel, DiskPageCompression compression) {
        return compressLevel != null ? checkCompressionLevelBounds(compressLevel, compression) :
            getDefaultCompressionLevel(compression);
    }

    /**
     * @param compression Compression algorithm.
     * @return Default compression level.
     */
    public static int getDefaultCompressionLevel(DiskPageCompression compression) {
        switch (compression) {
            case ZSTD:
                return ZSTD_DEFAULT_LEVEL;

            case LZ4:
                return LZ4_DEFAULT_LEVEL;

            case SNAPPY:
            case SKIP_GARBAGE:
            case DISABLED:
                return 0;
        }

        throw new IllegalArgumentException("Compression: " + compression);
    }

    /**
     * @param compressLevel Compression level.
     * @param compression Compression algorithm.
     * @return Compression level.
     */
    public static int checkCompressionLevelBounds(int compressLevel, DiskPageCompression compression) {
        switch (compression) {
            case ZSTD:
                checkCompressionLevelBounds(compressLevel, ZSTD_MIN_LEVEL, ZSTD_MAX_LEVEL);
                break;

            case LZ4:
                checkCompressionLevelBounds(compressLevel, LZ4_MIN_LEVEL, LZ4_MAX_LEVEL);
                break;

            default:
                throw new IllegalArgumentException("Compression level for " + compression + " is not supported.");
        }

        return compressLevel;
    }

    /**
     * @param compressLevel Compression level.
     * @param min Min level.
     * @param max Max level.
     */
    private static void checkCompressionLevelBounds(int compressLevel, int min, int max) {
        if (compressLevel < min || compressLevel > max) {
            throw new IllegalArgumentException("Compression level for LZ4 must be between " + min +
                " and " + max + ".");
        }
    }

    /**
     * @throws IgniteCheckedException Always.
     */
    private static <T> T fail() throws IgniteCheckedException {
        throw new IgniteCheckedException("Make sure that ignite-compress module is in classpath.");
    }

    /**
     * Checks weither page compression can be used for page file storage.
     *
     * @throws IgniteCheckedException If compression is not supported.
     */
    public void checkPageCompressionSupported() throws IgniteCheckedException {
        fail();
    }

    /**
     * Checks weither page file storage supports compression.
     *
     * @param storagePath Storage path.
     * @param pageSize Page size.
     * @throws IgniteCheckedException If compression is not supported.
     */
    public void checkPageCompressionSupported(Path storagePath, int pageSize) throws IgniteCheckedException {
        fail();
    }

    /**
     * @param page Page.
     * @param compactSize Compacted page size.
     * @return The given page.
     */
    protected static ByteBuffer setCompactionInfo(ByteBuffer page, int compactSize) {
        return setCompressionInfo(page, SKIP_GARBAGE, compactSize, compactSize);
    }

    /**
     * @param page Page.
     * @param compression Compression algorithm.
     * @param compressedSize Compressed size.
     * @param compactedSize Compact size.
     * @return The given page.
     */
    protected static ByteBuffer setCompressionInfo(
        ByteBuffer page,
        DiskPageCompression compression,
        int compressedSize,
        int compactedSize
    ) {
        assert compressedSize >= 0 && compressedSize <= Short.MAX_VALUE : compressedSize;
        assert compactedSize >= 0 && compactedSize <= Short.MAX_VALUE : compactedSize;

        PageIO.setCompressionType(page, getCompressionType(compression));
        PageIO.setCompressedSize(page, (short)compressedSize);
        PageIO.setCompactedSize(page, (short)compactedSize);

        return page;
    }

    /**
     * @param compression Compression.
     * @return Level.
     */
    private static byte getCompressionType(DiskPageCompression compression) {
        if (compression == DiskPageCompression.DISABLED)
            return UNCOMPRESSED_PAGE;

        switch (compression) {
            case ZSTD:
                return ZSTD_COMPRESSED_PAGE;

            case LZ4:
                return LZ4_COMPRESSED_PAGE;

            case SNAPPY:
                return SNAPPY_COMPRESSED_PAGE;

            case SKIP_GARBAGE:
                return COMPACTED_PAGE;
        }
        throw new IllegalStateException("Unexpected compression: " + compression);
    }

    /**
     * @param page Page buffer.
     * @param pageSize Page size.
     * @param blockSize Store block size.
     * @param compression Compression algorithm.
     * @param compressLevel Compression level.
     * @return Possibly compressed buffer.
     * @throws IgniteCheckedException If failed.
     */
    public ByteBuffer compressPage(
        ByteBuffer page,
        int pageSize,
        int blockSize,
        DiskPageCompression compression,
        int compressLevel
    ) throws IgniteCheckedException {
        assert compression != null && compression != DiskPageCompression.DISABLED : compression;
        assert U.isPow2(blockSize) : blockSize;
        assert page.position() == 0 && page.limit() >= pageSize;

        int oldPageLimit = page.limit();

        try {
            // Page size will be less than page limit when TDE is enabled. To make compaction and compression work
            // correctly we need to set limit to real page size.
            page.limit(pageSize);

            ByteBuffer compactPage = doCompactPage(page, pageSize);

            int compactSize = compactPage.limit();

            assert compactSize <= pageSize : compactSize;

            // If no need to compress further or configured just to skip garbage.
            if (compactSize < blockSize || compression == SKIP_GARBAGE)
                return setCompactionInfo(compactPage, compactSize);

            ByteBuffer compressedPage = doCompressPage(compression, compactPage, compactSize, compressLevel);

            assert compressedPage.position() == 0;
            int compressedSize = compressedPage.limit();

            int freeCompactBlocks = (pageSize - compactSize) / blockSize;
            int freeCompressedBlocks = (pageSize - compressedSize) / blockSize;

            if (freeCompactBlocks >= freeCompressedBlocks) {
                if (freeCompactBlocks == 0)
                    return page; // No blocks will be released.

                return setCompactionInfo(compactPage, compactSize);
            }

            return setCompressionInfo(compressedPage, compression, compressedSize, compactSize);
        }
        finally {
            page.limit(oldPageLimit);
        }
    }

    /**
     * @param page Page buffer.
     * @param pageSize Page size.
     * @return Compacted page buffer.
     */
    protected ByteBuffer doCompactPage(ByteBuffer page, int pageSize) throws IgniteCheckedException {
        PageIO io = PageIO.getPageIO(page);

        ByteBuffer compactPage = compactBuf.get();

        if (io instanceof CompactablePageIO) {
            // Drop the garbage from the page.
            ((CompactablePageIO)io).compactPage(page, compactPage, pageSize);
        }
        else {
            // Direct buffer is required as output of this method.
            if (page.isDirect())
                return page;

            PageUtils.putBytes(GridUnsafe.bufferAddress(compactPage), 0, page.array());

            compactPage.limit(pageSize);
        }

        return compactPage;
    }

    /**
     * @param compression Compression algorithm.
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @param compressLevel Compression level.
     * @return Compressed page.
     */
    protected ByteBuffer doCompressPage(
        DiskPageCompression compression,
        ByteBuffer compactPage,
        int compactSize,
        int compressLevel
    ) {
        throw new IllegalStateException("Unsupported compression: " + compression);
    }

    /**
     * @param page Possibly compressed page buffer.
     * @param pageSize Page size.
     * @throws IgniteCheckedException If failed.
     */
    public void decompressPage(ByteBuffer page, int pageSize) throws IgniteCheckedException {
        assert page.capacity() >= pageSize : "capacity=" + page.capacity() + ", pageSize=" + pageSize;

        byte compressType = PageIO.getCompressionType(page);

        if (compressType == UNCOMPRESSED_PAGE)
            return; // Nothing to do.

        short compressedSize = PageIO.getCompressedSize(page);
        short compactSize = PageIO.getCompactedSize(page);

        assert compactSize <= pageSize && compactSize >= compressedSize;

        if (compressType == COMPACTED_PAGE) {
            // Just setup bounds before restoring the page.
            page.position(0).limit(compactSize);
        }
        else
            doDecompressPage(compressType, page, compressedSize, compactSize);

        PageIO io = PageIO.getPageIO(page);

        if (io instanceof CompactablePageIO)
            ((CompactablePageIO)io).restorePage(page, pageSize);
        else {
            assert compactSize == pageSize
                : "Wrong compacted page size [compactSize=" + compactSize + ", pageSize=" + pageSize + ']';
        }

        setCompressionInfo(page, DiskPageCompression.DISABLED, 0, 0);
    }

    /** */
    protected void doDecompressPage(int compressType, ByteBuffer page, int compressedSize, int compactSize) {
        throw new IllegalStateException("Unsupported compression: " + compressType);
    }
}
