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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.configuration.DataStorageConfiguration.MAX_PAGE_SIZE;
import static org.apache.ignite.configuration.DiskPageCompression.LZ4;
import static org.apache.ignite.configuration.DiskPageCompression.SKIP_GARBAGE;
import static org.apache.ignite.configuration.DiskPageCompression.SNAPPY;
import static org.apache.ignite.configuration.DiskPageCompression.ZSTD;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.LZ4_MAX_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.LZ4_MIN_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.UNCOMPRESSED_PAGE;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.ZSTD_DEFAULT_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.ZSTD_MAX_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessorImpl.allocateDirectBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;

/**
 */
@RunWith(Parameterized.class)
public class CompressionProcessorTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameters(name = "page_size = {0}, block_size = {1}, compression = {2}, level = {3}")
    public static Iterable<Object[]> testParameters() {
        List<Object[]> params = new ArrayList<>();

        for (int pageSz = 4 * 1024; pageSz <= MAX_PAGE_SIZE; pageSz *= 2) {
            for (int blockSize : Arrays.asList(16, 128, 1024, 2048)) {
                params.add(new Object[] {pageSz, blockSize, SKIP_GARBAGE, 0});
                params.add(new Object[] {pageSz, blockSize, SNAPPY, 0});
                params.add(new Object[] {pageSz, blockSize, LZ4, LZ4_MIN_LEVEL});
                params.add(new Object[] {pageSz, blockSize, LZ4, LZ4_MAX_LEVEL});
                params.add(new Object[] {pageSz, blockSize, ZSTD, ZSTD_DEFAULT_LEVEL});
                params.add(new Object[] {pageSz, blockSize, ZSTD, ZSTD_MAX_LEVEL});
            }
        }

        return params;
    }

    /** */
    @Parameterized.Parameter
    public int pageSize;

    /** */
    @Parameterized.Parameter(1)
    public int blockSize;

    /** */
    @Parameterized.Parameter(2)
    public DiskPageCompression compression;

    /** */
    @Parameterized.Parameter(3)
    public int compressLevel;

    /** */
    private CompressionProcessor p;

    /** */
    private TestInnerIO innerIo;

    /** */
    private TestLeafIO leafIo;

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        /**
         * 7 is divisor of 8192 - 58, 6 is divisor of 4096 - 58 and 16384 - 58. So an item size should be
         * 7 for a 8k page and 6 for 4k and 16k pages respectively in order to fill the page fully.
         * See {@see org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO#getMaxCount}
         */
        int itemSize = pageSize == 8192 ? 7 : 6;

        innerIo = new TestInnerIO(itemSize);
        leafIo = new TestLeafIO(itemSize);

        PageIO.registerTest(innerIo, leafIo);

        p = new CompressionProcessorImpl(new GridTestKernalContext(log));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testDataPage() throws IgniteCheckedException {
        Random rnd = ThreadLocalRandom.current();

        final byte[][] rows = new byte[][]{
            new byte[17], new byte[37], new byte[71]
        };

        for (int i = 0; i < rows.length; i++)
            rnd.nextBytes(rows[i]);

        ByteBuffer page = allocateDirectBuffer(pageSize);
        long pageAddr = bufferAddress(page);

        SimpleDataPageIO io = SimpleDataPageIO.VERSIONS.latest();

        long pageId = PageIdUtils.pageId(PageIdAllocator.MAX_PARTITION_ID, PageIdAllocator.FLAG_DATA, 171717);

        io.initNewPage(pageAddr, pageId, pageSize, null);

        checkIo(io, page);

        Function<ByteBuffer, List<Bytes>> getContents = (buf) -> {
            try {
                long addr = bufferAddress(buf);

                return io.forAllItems(addr, (link) -> {
                    DataPagePayload payload = io.readPayload(addr, PageIdUtils.itemId(link), pageSize);

                    return new Bytes(payload.getBytes(addr));
                });
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        };

        // Empty data page.
        checkCompressDecompress(page, getContents, false);

        GridIntList itemIds = new GridIntList();

        for (;;) {
            byte[] row = rows[rnd.nextInt(rows.length)];

            if (io.getFreeSpace(pageAddr) < row.length)
                break;

            itemIds.add(io.addRow(pageAddr, row, pageSize));
        }

        int freeSpace = io.getFreeSpace(pageAddr);

        if (freeSpace != 0) {
            byte[] lastRow = new byte[freeSpace];
            rnd.nextBytes(lastRow);

            io.addRowFragment(pageId, pageAddr, lastRow, 777L, pageSize);

            assertEquals(0, io.getRealFreeSpace(pageAddr));
        }

        // Full data page.
        checkCompressDecompress(page, getContents, io.getRealFreeSpace(pageAddr) == 0);

        for (int i = 0; i < itemIds.size(); i += 2)
            io.removeRow(pageAddr, itemIds.get(i), pageSize);

        // Half-filled data page.
        checkCompressDecompress(page, getContents, false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testInnerPage() throws IgniteCheckedException {
        doTestBTreePage(innerIo);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testLeafPage() throws IgniteCheckedException {
        doTestBTreePage(leafIo);
    }

    /**
     * @param io Page IO.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestBTreePage(BPlusIO<byte[]> io) throws IgniteCheckedException {
        Random rnd = ThreadLocalRandom.current();

        final byte[][] rows = new byte[3][io.getItemSize()];

        for (int i = 0; i < rows.length; i++)
            rnd.nextBytes(rows[i]);

        ByteBuffer page = allocateDirectBuffer(pageSize);
        long pageAddr = bufferAddress(page);

        long pageId = PageIdUtils.pageId(PageIdAllocator.INDEX_PARTITION, PageIdAllocator.FLAG_IDX, 171717);

        io.initNewPage(pageAddr, pageId, pageSize, null);

        checkIo(io, page);

        Function<ByteBuffer, List<?>> getContents = (buf) -> {
            long addr = bufferAddress(buf);

            int cnt = io.getCount(addr);

            List<Object> list = new ArrayList<>(cnt);

            for (int i = 0; i < cnt; i++) {
                if (!io.isLeaf())
                    list.add(((BPlusInnerIO)io).getLeft(addr, i));

                try {
                    list.add(new Bytes(io.getLookupRow(null, addr, i)));
                }
                catch (IgniteCheckedException e) {
                    throw new IllegalStateException(e);
                }

                if (!io.isLeaf())
                    list.add(((BPlusInnerIO)io).getRight(addr, i));
            }

            return list;
        };

        // Empty page.
        checkCompressDecompress(page, getContents, false);

        int cnt = io.getMaxCount(pageAddr, pageSize);

        for (int i = 0; i < cnt; i++) {
            byte[] row = rows[rnd.nextInt(rows.length)];
            io.insert(pageAddr, i, row, row, 777_000 + i, false);
        }

        if (io.isLeaf())
            assertEquals(pageSize, io.getItemsEnd(pageAddr)); // Page must be full.

        // Full page.
        checkCompressDecompress(page, getContents, io.isLeaf());

        io.setCount(pageAddr, cnt / 2);

        // Half page.
        checkCompressDecompress(page, getContents, false);
    }

    /** */
    private void checkIo(PageIO io, ByteBuffer page) throws IgniteCheckedException {
        assertSame(io, PageIO.getPageIO(bufferAddress(page)));
        assertSame(io, PageIO.getPageIO(page));
    }

    /** */
    private void checkCompressDecompress(ByteBuffer page, Function<ByteBuffer, ?> getPageContents, boolean fullPage)
        throws IgniteCheckedException {
        PageIO.setCrc(page, 0xABCDEF13);

        long pageId = PageIO.getPageId(page);
        PageIO io = PageIO.getPageIO(page);

        ByteBuffer compressed = p.compressPage(page, pageSize, blockSize, compression, compressLevel);

        int compressedSize = PageIO.getCompressedSize(compressed);

        assertNotSame(page, compressed); // This is generally possible but not interesting in this test.

        assertTrue(compressedSize > 0);
        assertTrue(compressedSize <= pageSize);
        assertEquals(compressedSize, compressed.limit());

        if (!fullPage || compression != SKIP_GARBAGE)
            assertTrue(pageSize > compressedSize);

        assertEquals(0, compressed.position());

        checkIo(io, compressed);
        assertEquals(0, page.position());
        assertEquals(pageSize, page.limit());

        info(io.getClass().getSimpleName() + " " + compression + " " + compressLevel + ": " + compressedSize + "/" + pageSize);

        if (!fullPage || compression != SKIP_GARBAGE)
            assertTrue(compressedSize < pageSize);

        assertEquals(pageId, PageIO.getPageId(compressed));

        ByteBuffer decompress = allocateDirectBuffer(pageSize);
        decompress.put(compressed).clear();

        p.decompressPage(decompress, pageSize);

        assertEquals(0, decompress.position());
        assertEquals(pageSize, decompress.limit());

        checkIo(io, decompress);
        assertEquals(UNCOMPRESSED_PAGE, PageIO.getCompressionType(page));
        assertEquals(0, PageIO.getCompressedSize(page));
        assertEquals(0, PageIO.getCompactedSize(page));

        assertTrue(Arrays.equals(getPageCommonHeader(page), getPageCommonHeader(decompress)));
        assertEquals(getPageContents.apply(page), getPageContents.apply(decompress));
    }

    /**
     * @param page Page.
     * @return Page header.
     */
    private static byte[] getPageCommonHeader(ByteBuffer page) {
        return PageUtils.getBytes(GridUnsafe.bufferAddress(page), 0, PageIO.COMMON_HEADER_END);
    }

    /**
     */
    private static class Bytes {
        /** */
        private final byte[] bytes;

        /**
         * @param bytes Bytes.
         */
        private Bytes(byte[] bytes) {
            assert bytes != null;
            this.bytes = bytes;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Bytes bytes1 = (Bytes)o;

            return Arrays.equals(bytes, bytes1.bytes);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    /**
     */
    static class TestLeafIO extends BPlusLeafIO<byte[]> {
        /**
         */
        TestLeafIO(int itemSz) {
            super(29_501, 1, itemSz);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, byte[] row) {
            PageUtils.putBytes(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<byte[]> srcIo, long srcPageAddr,
            int srcIdx) throws IgniteCheckedException {
            storeByOffset(dstPageAddr, offset(dstIdx), srcIo.getLookupRow(null, srcPageAddr, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public byte[] getLookupRow(BPlusTree<byte[], ?> tree, long pageAddr, int idx) {
            return PageUtils.getBytes(pageAddr, offset(idx), itemSize);
        }
    }

    /**
     */
    static class TestInnerIO extends BPlusInnerIO<byte[]> {
        /**
         */
        TestInnerIO(int itemSz) {
            super(29_502, 1, true, itemSz);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, byte[] row) {
            PageUtils.putBytes(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<byte[]> srcIo, long srcPageAddr,
            int srcIdx) throws IgniteCheckedException {
            storeByOffset(dstPageAddr, offset(dstIdx), srcIo.getLookupRow(null, srcPageAddr, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public byte[] getLookupRow(BPlusTree<byte[], ?> tree, long pageAddr, int idx) {
            return PageUtils.getBytes(pageAddr, offset(idx), itemSize);
        }
    }
}

