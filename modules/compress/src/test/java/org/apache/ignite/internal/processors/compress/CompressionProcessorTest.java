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
import org.apache.ignite.configuration.PageCompression;
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
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.PageCompression.SKIP_GARBAGE;
import static org.apache.ignite.configuration.PageCompression.LZ4;
import static org.apache.ignite.configuration.PageCompression.ZSTD;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.LZ4_MAX_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.LZ4_MIN_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.ZSTD_MAX_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessorImpl.allocateDirectBuffer;
import static org.apache.ignite.internal.processors.compress.CompressionProcessorTest.TestInnerIO.INNER_IO;
import static org.apache.ignite.internal.processors.compress.CompressionProcessorTest.TestLeafIO.LEAF_IO;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;

/**
 */
public class CompressionProcessorTest extends GridCommonAbstractTest {
    /** */
    private static final int ITEM_SIZE = 6; // To fill the whole page.

    /** */
    private int blockSize = 16;

    /** */
    private int pageSize = 4 * 1024;

    /** */
    private PageCompression compression;

    /** */
    private int compressLevel;

    /** */
    private CompressionProcessor p;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        PageIO.registerTest(INNER_IO, LEAF_IO);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        p = new CompressionProcessorImpl(new GridTestKernalContext(log));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageCompact16() throws IgniteCheckedException {
        blockSize = 16;
        compression = SKIP_GARBAGE;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageCompact128() throws IgniteCheckedException {
        blockSize = 128;
        compression = SKIP_GARBAGE;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageCompact1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = SKIP_GARBAGE;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageCompact2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = SKIP_GARBAGE;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageZstd16() throws IgniteCheckedException {
        blockSize = 16;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageZstd128() throws IgniteCheckedException {
        blockSize = 128;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageZstd1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageZstd2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageLz4Fast16() throws IgniteCheckedException {
        blockSize = 16;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageLz4Fast128() throws IgniteCheckedException {
        blockSize = 128;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageLz4Fast1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageLz4Fast2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageLz4Slow16() throws IgniteCheckedException {
        blockSize = 16;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageLz4Slow128() throws IgniteCheckedException {
        blockSize = 128;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageLz4Slow1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageLz4Slow2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageCompact16() throws IgniteCheckedException {
        blockSize = 16;
        compression = SKIP_GARBAGE;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageCompact16() throws IgniteCheckedException {
        blockSize = 16;
        compression = SKIP_GARBAGE;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageZstd16() throws IgniteCheckedException {
        blockSize = 16;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageZstd16() throws IgniteCheckedException {
        blockSize = 16;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestBTreePage(LEAF_IO);
    }


    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageLz4Fast16() throws IgniteCheckedException {
        blockSize = 16;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageLz4Fast16() throws IgniteCheckedException {
        blockSize = 16;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageLz4Slow16() throws IgniteCheckedException {
        blockSize = 16;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageLz4Slow16() throws IgniteCheckedException {
        blockSize = 16;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageCompact128() throws IgniteCheckedException {
        blockSize = 128;
        compression = SKIP_GARBAGE;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageCompact128() throws IgniteCheckedException {
        blockSize = 128;
        compression = SKIP_GARBAGE;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageZstd128() throws IgniteCheckedException {
        blockSize = 128;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageZstd128() throws IgniteCheckedException {
        blockSize = 128;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageLz4Fast128() throws IgniteCheckedException {
        blockSize = 128;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageLz4Fast128() throws IgniteCheckedException {
        blockSize = 128;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageLz4Slow128() throws IgniteCheckedException {
        blockSize = 128;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageLz4Slow128() throws IgniteCheckedException {
        blockSize = 128;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageCompact1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = SKIP_GARBAGE;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageCompact1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = SKIP_GARBAGE;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageZstd1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageZstd1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageLz4Fast1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageLz4Fast1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageLz4Slow1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageLz4Slow1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageCompact2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = SKIP_GARBAGE;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageCompact2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = SKIP_GARBAGE;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageZstd2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageZstd2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = ZSTD;
        compressLevel = ZSTD_MAX_LEVEL;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageLz4Fast2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageLz4Fast2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = LZ4;
        compressLevel = LZ4_MIN_LEVEL;

        doTestBTreePage(LEAF_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testInnerPageLz4Slow2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestBTreePage(INNER_IO);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLeafPageLz4Slow2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = LZ4;
        compressLevel = LZ4_MAX_LEVEL;

        doTestBTreePage(LEAF_IO);
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

        io.initNewPage(pageAddr, pageId, pageSize);

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

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void doTestDataPage() throws IgniteCheckedException {
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

        io.initNewPage(pageAddr, pageId, pageSize);

        checkIo(io, page);

        Function<ByteBuffer,List<Bytes>> getContents = (buf) -> {
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

    private void checkIo(PageIO io, ByteBuffer page) throws IgniteCheckedException {
        assertSame(io, PageIO.getPageIO(bufferAddress(page)));
        assertSame(io, PageIO.getPageIO(page));
    }

    private void checkCompressDecompress(ByteBuffer page, Function<ByteBuffer, ?> getPageContents, boolean fullPage)
        throws IgniteCheckedException {
        long pageId = PageIO.getPageId(page);
        PageIO io = PageIO.getPageIO(page);

        ByteBuffer compressed = p.compressPage(page, pageSize, blockSize, compression, compressLevel);

        int compressedSize = compressed.remaining();

        checkIo(io, compressed);
        assertNotSame(page, compressed);
        assertEquals(0, page.position());
        assertEquals(pageSize, page.limit());

        info(io.getClass().getSimpleName() + " " + compression + " " + compressLevel + ": " + compressedSize + "/" + pageSize);

        if (!fullPage || compression != SKIP_GARBAGE)
            assertTrue(compressedSize < pageSize);

        assertEquals(pageId, PageIO.getPageId(compressed));

        ByteBuffer decompress = allocateDirectBuffer(pageSize);
        decompress.put(compressed);
        decompress.flip();

        p.decompressPage(decompress, pageSize);

        assertEquals(0, decompress.position());
        assertEquals(pageSize, decompress.limit());

        checkIo(io, decompress);

        if (getPageContents != null)
            assertEquals(getPageContents.apply(page), getPageContents.apply(decompress));
    }

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
        /** */
        static final TestLeafIO LEAF_IO = new TestLeafIO();

        /**
         */
        TestLeafIO() {
            super(29_501, 1, ITEM_SIZE);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, byte[] row) throws IgniteCheckedException {
            PageUtils.putBytes(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<byte[]> srcIo, long srcPageAddr,
            int srcIdx) throws IgniteCheckedException {
            storeByOffset(dstPageAddr, offset(dstIdx), srcIo.getLookupRow(null, srcPageAddr, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public byte[] getLookupRow(BPlusTree<byte[],?> tree, long pageAddr, int idx) throws IgniteCheckedException {
            return PageUtils.getBytes(pageAddr, offset(idx), itemSize);
        }
    }

    /**
     */
    static class TestInnerIO extends BPlusInnerIO<byte[]> {
        /** */
        static TestInnerIO INNER_IO = new TestInnerIO();

        /**
         */
        TestInnerIO() {
            super(29_502, 1, true, ITEM_SIZE);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, byte[] row) throws IgniteCheckedException {
            PageUtils.putBytes(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<byte[]> srcIo, long srcPageAddr,
            int srcIdx) throws IgniteCheckedException {
            storeByOffset(dstPageAddr, offset(dstIdx), srcIo.getLookupRow(null, srcPageAddr, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public byte[] getLookupRow(BPlusTree<byte[],?> tree, long pageAddr, int idx) throws IgniteCheckedException {
            return PageUtils.getBytes(pageAddr, offset(idx), itemSize);
        }
    }
}
