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

import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.PageCompression;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CompactablePageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.configuration.PageCompression.DROP_GARBAGE;
import static org.apache.ignite.internal.util.GridUnsafe.NATIVE_BYTE_ORDER;

/**
 * Compression processor.
 */
@SuppressWarnings("unused")
public class CompressionProcessorImpl extends CompressionProcessor {
    /** */
    private final ThreadLocal<ByteBuffer> tmp = new ThreadLocal<ByteBuffer>() {
        @Override protected ByteBuffer initialValue() {
            return allocateDirectBuffer(32 * 1024);
        }

        @Override public ByteBuffer get() {
            ByteBuffer buf = super.get();
            buf.clear();
            return buf;
        }
    };

    /**
     * @param ctx Kernal context.
     */
    public CompressionProcessorImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param cap Capacity.
     * @return Direct byte buffer.
     */
    static ByteBuffer allocateDirectBuffer(int cap) {
        return ByteBuffer.allocateDirect(cap).order(NATIVE_BYTE_ORDER);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer compressPage(
        ByteBuffer page,
        int fsBlockSize,
        PageCompression compression,
        int compressLevel
    ) throws IgniteCheckedException {
        assert page.position() == 0 && page.limit() == page.capacity();
        assert compression != null;

        if (!U.isPow2(fsBlockSize))
            return page; // Our pages will be misaligned.

        PageIO io = PageIO.getPageIO(page);

        if (!(io instanceof CompactablePageIO))
            return page;

        int pageSize = page.remaining();

        assert U.isPow2(pageSize): pageSize;

        if (pageSize < fsBlockSize * 2)
            return page; // Makes no sense to compress the page, we will not free any disk space.

        ByteBuffer compactPage = tmp.get();

        try {
            ((CompactablePageIO)io).compactPage(page, compactPage);
        }
        finally {
            page.clear();
        }

        int compactSize = compactPage.limit();

        if (compactSize < fsBlockSize || compression == DROP_GARBAGE) {
            // No need to compress further or configured just to drop garbage.
            setCompressionInfo(compactPage, DROP_GARBAGE, compactSize);

            // Can not return thread local buffer, because the actual write may be async.
            ByteBuffer res = allocateDirectBuffer(compactSize);
            res.put(compactPage).flip();
            return res;
        }

        ByteBuffer compressedPage = compressPage(compression, compactPage, compactSize, compressLevel);

        int compressedSize = compressedPage.limit();

        if (pageSize - compressedSize < fsBlockSize)
            return page; // Were not able to release file blocks.

        setCompressionInfo(compressedPage, compression, compressedSize);

        return compressedPage;
    }

    /**
     * @param page Page.
     * @param compression Compression algorithm.
     * @param compressedSize Compressed size.
     */
    private static void setCompressionInfo(ByteBuffer page, PageCompression compression, int compressedSize) {
        assert compressedSize >= 0 && compressedSize < Short.MAX_VALUE;

        PageIO.setCompressionType(page, getCompressionType(compression));
        PageIO.setCompressedSize(page, (short)compressedSize);
    }

    /**
     * @param compression Compression algorithm.
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @param compressLevel Compression level.
     * @return Compressed page.
     */
    private static ByteBuffer compressPage(PageCompression compression, ByteBuffer compactPage, int compactSize, int compressLevel) {
        switch (compression) {
            case ZSTD:
                return compressPageZstd(compactPage, compactSize, compressLevel);

            case LZ4:
                return compressPageLz4(compactPage, compactSize, compressLevel);
        }
        throw new IllegalStateException("Unsupported compression: " + compression);
    }

    /**
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @param compressLevel Compression level.
     * @return Compressed page.
     */
    private static ByteBuffer compressPageLz4(ByteBuffer compactPage, int compactSize, int compressLevel) {
        LZ4Factory lz4 = LZ4Factory.fastestInstance();
        LZ4Compressor compressor;

        if (compressLevel == 0)
            compressor = lz4.fastCompressor();
        else
            compressor = lz4.highCompressor(compressLevel);

        ByteBuffer compressedPage = allocateDirectBuffer(PageIO.COMMON_HEADER_END +
            compressor.maxCompressedLength(compactSize - PageIO.COMMON_HEADER_END));

        copyPageHeader(compactPage, compressedPage, compactSize);
        compressor.compress(compactPage, compressedPage);
        compressedPage.flip();

        return compressedPage;
    }

    /**
     * @param compactPage Compacted page.
     * @param compactSize Compacted page size.
     * @param compressLevel Compression level.
     * @return Compressed page.
     */
    private static ByteBuffer compressPageZstd(ByteBuffer compactPage, int compactSize, int compressLevel) {
        ByteBuffer compressedPage = allocateDirectBuffer((int)(PageIO.COMMON_HEADER_END +
            Zstd.compressBound(compactSize - PageIO.COMMON_HEADER_END)));

        copyPageHeader(compactPage, compressedPage, compactSize);
        Zstd.compress(compressedPage, compactPage, compressLevel);
        compressedPage.flip();

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

    /**
     * @param compression Compression.
     * @return Level.
     */
    private static byte getCompressionType(PageCompression compression) {
        if (compression == null)
            return UNCOMPRESSED_PAGE;

        switch (compression) {
            case ZSTD:
                return ZSTD_COMPRESSED_PAGE;

            case LZ4:
                return LZ4_COMPRESSED_PAGE;

            case DROP_GARBAGE:
                return COMPACTED_PAGE;
        }
        throw new IllegalStateException("Unexpected compression: " + compression);
    }

    /** {@inheritDoc} */
    @Override public void decompressPage(ByteBuffer page) throws IgniteCheckedException {
        final int pageSize = page.capacity();

        byte compressType = PageIO.getCompressionType(page);
        short compressedSize = PageIO.getCompressedSize(page);

        switch (compressType) {
            case UNCOMPRESSED_PAGE:
                return;

            case COMPACTED_PAGE:
                break;

            case ZSTD_COMPRESSED_PAGE:
                decompressPageZstd(page, compressedSize, pageSize);
                break;

            case LZ4_COMPRESSED_PAGE:
                decompressPageLz4(page, compressedSize, pageSize);
                break;

            default:
                throw new IllegalStateException("Unknown compression type: " + compressType);
        }

        CompactablePageIO io = PageIO.getPageIO(page);

        io.restorePage(page, pageSize);

        setCompressionInfo(page, null, 0);
    }

    /**
     * @param page Page.
     * @param compressedSize Compressed size.
     * @param pageSize Page size.
     */
    private void decompressPageZstd(ByteBuffer page, int compressedSize, int pageSize) {
        assert page.isDirect();

        ByteBuffer dst = tmp.get();

        page.position(PageIO.COMMON_HEADER_END).limit(compressedSize);
        Zstd.decompress(dst, page);
        dst.flip();

        page.position(PageIO.COMMON_HEADER_END).limit(pageSize);
        page.put(dst).flip();
    }

    /**
     * @param page Page.
     * @param compressedSize Compressed size.
     * @param pageSize Page size.
     */
    private void decompressPageLz4(ByteBuffer page, int compressedSize, int pageSize) {
        LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();

        ByteBuffer dst = tmp.get();

        // LZ4 needs this limit to be exact.
        dst.limit(pageSize - PageIO.COMMON_HEADER_END);

        page.position(PageIO.COMMON_HEADER_END).limit(compressedSize);
        decompressor.decompress(page, dst);
        dst.flip();

        page.position(PageIO.COMMON_HEADER_END).limit(pageSize);
        page.put(dst).flip();
    }
}
