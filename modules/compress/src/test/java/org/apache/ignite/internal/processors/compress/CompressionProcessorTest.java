package org.apache.ignite.internal.processors.compress;

import java.nio.ByteBuffer;
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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.PageCompression.DROP_GARBAGE;
import static org.apache.ignite.configuration.PageCompression.ZSTD;
import static org.apache.ignite.internal.processors.compress.CompressionProcessorImpl.allocateDirectBuffer;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;

/**
 */
public class CompressionProcessorTest extends GridCommonAbstractTest {
    /** */
    private int blockSize = 16;

    /** */
    private int pageSize = 4 * 1024;

    /** */
    private PageCompression compression = DROP_GARBAGE;

    /** */
    private int compressLevel = 0;

    /** */
    private CompressionProcessor p;

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        p = new CompressionProcessorImpl(new GridTestKernalContext(log));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageCompact16() throws IgniteCheckedException {
        blockSize = 16;
        compression = DROP_GARBAGE;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageCompact128() throws IgniteCheckedException {
        blockSize = 128;
        compression = DROP_GARBAGE;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageCompact1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = DROP_GARBAGE;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageCompact2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = DROP_GARBAGE;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageZstd16() throws IgniteCheckedException {
        blockSize = 16;
        compression = ZSTD;
        compressLevel = 19;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageZstd128() throws IgniteCheckedException {
        blockSize = 128;
        compression = ZSTD;
        compressLevel = 19;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageZstd1k() throws IgniteCheckedException {
        blockSize = 1024;
        compression = ZSTD;
        compressLevel = 19;

        doTestDataPage();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testDataPageZstd2k() throws IgniteCheckedException {
        blockSize = 2 * 1024;
        compression = ZSTD;
        compressLevel = 19;

        doTestDataPage();
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
        int pageSize = page.remaining();
        long pageId = PageIO.getPageId(page);
        PageIO io = PageIO.getPageIO(page);

        ByteBuffer compressed = p.compressPage(page, blockSize, compression, compressLevel);
        int compressedSize = compressed.remaining();

        checkIo(io, compressed);
        assertNotSame(page, compressed);
        assertEquals(0, page.position());
        assertEquals(pageSize, page.limit());

        if (!fullPage || compression != DROP_GARBAGE)
            assertTrue(compressedSize < pageSize);

        assertEquals(pageId, PageIO.getPageId(compressed));

        ByteBuffer decompress = allocateDirectBuffer(pageSize);
        decompress.put(compressed);
        decompress.flip();

        p.decompressPage(decompress);

        assertEquals(0, decompress.position());
        assertEquals(pageSize, decompress.limit());

        checkIo(io, decompress);

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
}
