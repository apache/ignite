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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.ThreadLocalDirectByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.xerial.snappy.Snappy;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.configuration.DataStorageConfiguration.MAX_PAGE_SIZE;
import static org.apache.ignite.internal.util.GridUnsafe.NATIVE_BYTE_ORDER;

/**
 * Compression processor.
 */
public class CompressionProcessorImpl extends CompressionProcessor {
    /** A bit more than max page size, extra space is required by compressors. */
    private final ThreadLocalDirectByteBuffer compressBuf =
        new ThreadLocalDirectByteBuffer(maxCompressedBufferSize(MAX_PAGE_SIZE), NATIVE_BYTE_ORDER);

    /**
     * @param ctx Kernal context.
     */
    @SuppressWarnings("WeakerAccess")
    public CompressionProcessorImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void checkPageCompressionSupported() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void checkPageCompressionSupported(Path storagePath, int pageSize) throws IgniteCheckedException {
        if (!U.isLinux())
            throw new IgniteCheckedException("Currently page compression is supported only for Linux.");

        FileSystemUtils.checkSupported();

        int fsBlockSize = FileSystemUtils.getFileSystemBlockSize(storagePath);

        if (fsBlockSize <= 0)
            throw new IgniteCheckedException("Failed to get file system block size: " + storagePath);

        if (!U.isPow2(fsBlockSize))
            throw new IgniteCheckedException("Storage block size must be power of 2: " + fsBlockSize);

        if (pageSize < fsBlockSize * 2) {
            throw new IgniteCheckedException("Page size (now configured to " + pageSize + " bytes) " +
                "must be at least 2 times larger than the underlying storage block size (detected to be " + fsBlockSize +
                " bytes at '" + storagePath + "') for page compression.");
        }

        checkPunchHole(storagePath, fsBlockSize);
    }

    /** Check if filesystem actually supports punching holes. */
    private void checkPunchHole(Path storagePath, int fsBlockSz) throws IgniteException {
        ByteBuffer buf = null;
        File testFile = null;
        try {
            testFile = File.createTempFile("punch_hole_", null, storagePath.toFile());

            buf = GridUnsafe.allocateBuffer(fsBlockSz * 2);
            GridUnsafe.zeroMemory(GridUnsafe.bufferAddress(buf), buf.capacity());

            try (RandomAccessFileIO testFileIO = new RandomAccessFileIO(testFile, CREATE, WRITE)) {
                testFileIO.writeFully(buf);

                testFileIO.punchHole(fsBlockSz, fsBlockSz);
            }
        }
        catch (Exception e) {
            throw new IgniteException("File system does not support punching holes on path " + storagePath, e);
        }
        finally {
            if (buf != null)
                GridUnsafe.freeBuffer(buf);

            if (testFile != null)
                testFile.delete();
        }
    }

    /**
     * @param compression Compression algorithm.
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @param compressLevel Compression level.
     * @return Compressed page.
     */
    @Override protected ByteBuffer doCompressPage(
        DiskPageCompression compression,
        ByteBuffer compactPage,
        int compactSize,
        int compressLevel
    ) {
        switch (compression) {
            case ZSTD:
                return compressPageZstd(compactPage, compactSize, compressLevel);

            case LZ4:
                return compressPageLz4(compactPage, compactSize, compressLevel);

            case SNAPPY:
                return compressPageSnappy(compactPage, compactSize);
        }
        throw new IllegalStateException("Unsupported compression: " + compression);
    }

    /**
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @param compressLevel Compression level.
     * @return Compressed page.
     */
    private ByteBuffer compressPageLz4(ByteBuffer compactPage, int compactSize, int compressLevel) {
        LZ4Compressor compressor = Lz4.getCompressor(compressLevel);

        ByteBuffer compressedPage = compressBuf.get();

        copyPageHeader(compactPage, compressedPage, compactSize);
        compressor.compress(compactPage, compressedPage);

        compactPage.flip();
        compressedPage.flip();

        return compressedPage;
    }

    /**
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @param compressLevel Compression level.
     * @return Compressed page.
     */
    private ByteBuffer compressPageZstd(ByteBuffer compactPage, int compactSize, int compressLevel) {
        ByteBuffer compressedPage = compressBuf.get();

        copyPageHeader(compactPage, compressedPage, compactSize);
        Zstd.compress(compressedPage, compactPage, compressLevel);

        compactPage.flip();
        compressedPage.flip();

        return compressedPage;
    }

    /**
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @return Compressed page.
     */
    private ByteBuffer compressPageSnappy(ByteBuffer compactPage, int compactSize) {
        ByteBuffer compressedPage = compressBuf.get();

        copyPageHeader(compactPage, compressedPage, compactSize);

        try {
            int compressedSize = Snappy.compress(compactPage, compressedPage);
            assert compressedPage.limit() == PageIO.COMMON_HEADER_END + compressedSize;
        }
        catch (IOException e) {
            throw new IgniteException("Failed to compress page with Snappy.", e);
        }

        compactPage.position(0);
        compressedPage.position(0);

        return compressedPage;
    }

    /**
     * @param compactPage Compacted page.
     * @param compressedPage Compressed page.
     * @param compactSize Compacted page size.
     */
    private static void copyPageHeader(ByteBuffer compactPage, ByteBuffer compressedPage, int compactSize) {
        compactPage.limit(PageIO.COMMON_HEADER_END);
        compressedPage.put(compactPage);
        compactPage.limit(compactSize);
    }

    /** {@inheritDoc} */
    @Override protected void doDecompressPage(int compressType, ByteBuffer page, int compressedSize, int compactSize) {
        ByteBuffer dst = compressBuf.get();

        // Position on a part that needs to be decompressed.
        page.limit(compressedSize)
            .position(PageIO.COMMON_HEADER_END);

        // LZ4 needs this limit to be exact.
        dst.limit(compactSize - PageIO.COMMON_HEADER_END);

        switch (compressType) {
            case ZSTD_COMPRESSED_PAGE:
                Zstd.decompress(dst, page);
                dst.flip();

                break;

            case LZ4_COMPRESSED_PAGE:
                Lz4.decompress(page, dst);
                dst.flip();

                break;

            case SNAPPY_COMPRESSED_PAGE:
                try {
                    Snappy.uncompress(page, dst);
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }
                break;

            default:
                throw new IgniteException("Unknown compression: " + compressType);
        }

        page.position(PageIO.COMMON_HEADER_END).limit(compactSize);
        page.put(dst).flip();
        assert page.limit() == compactSize;
    }

    /** */
    private static int maxCompressedBufferSize(int baseSz) {
        int lz4Sz = Lz4.fastCompressor.maxCompressedLength(baseSz);
        int zstdSz = (int)Zstd.compressBound(baseSz);
        int snappySz = Snappy.maxCompressedLength(baseSz);

        return Math.max(Math.max(lz4Sz, zstdSz), snappySz);
    }

    /** */
    static class Lz4 {
        /** */
        static final LZ4Factory factory = LZ4Factory.fastestInstance();

        /** */
        static final LZ4FastDecompressor decompressor = factory.fastDecompressor();

        /** */
        static final LZ4Compressor fastCompressor = factory.fastCompressor();

        /**
         * @param level Compression level.
         * @return Compressor.
         */
        static LZ4Compressor getCompressor(int level) {
            assert level >= 0 && level <= 17 : level;
            return level == 0 ? fastCompressor : factory.highCompressor(level);
        }

        /**
         * @param page Page.
         * @param dst Destination buffer.
         */
        static void decompress(ByteBuffer page, ByteBuffer dst) {
            decompressor.decompress(page, dst);
        }
    }
}
