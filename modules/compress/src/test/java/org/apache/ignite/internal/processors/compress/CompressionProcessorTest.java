package org.apache.ignite.internal.processors.compress;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.PageCompression;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.PageCompression.DROP_GARBAGE;
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
    @Override protected void beforeTestsStarted() {
        p = new CompressionProcessorImpl(new GridTestKernalContext(log));
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
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

        io.initNewPage(pageAddr, pageId, pageSize);

        checkIo(io, page);

        ByteBuffer buf = checkCompressDecompress(page);
        checkIo(io, buf);
        assertEquals(Collections.emptyList(), io.forAllItems(bufferAddress(buf), (link) -> link));

        GridIntList itemIds = new GridIntList();

        for (;;) {
            byte[] row = rows[rnd.nextInt(rows.length)];

            if (io.getFreeSpace(pageAddr) < row.length)
                break;

            itemIds.add(io.addRow(pageAddr, row, pageSize));
        }

        List<Long> links = io.forAllItems(pageAddr, (link) -> link);

        buf = checkCompressDecompress(page);
        checkIo(io, buf);
        assertEquals(links, io.forAllItems(bufferAddress(buf), (link) -> link));

        for (int i = 0; i < itemIds.size(); i += 2)
            io.removeRow(pageAddr, itemIds.get(i), pageSize);

        links = io.forAllItems(pageAddr, (link) -> link);
        assertFalse(links.isEmpty());

        buf = checkCompressDecompress(page);
        checkIo(io, buf);
        assertEquals(links, io.forAllItems(bufferAddress(buf), (link) -> link));
    }

    private void checkIo(PageIO io, ByteBuffer page) throws IgniteCheckedException {
        assertSame(io, PageIO.getPageIO(bufferAddress(page)));
        assertSame(io, PageIO.getPageIO(page));
    }

    private ByteBuffer checkCompressDecompress(ByteBuffer page) throws IgniteCheckedException {
        int pageSize = page.remaining();
        long pageId = PageIO.getPageId(page);

        ByteBuffer compacted = p.compressPage(page, blockSize, compression, compressLevel);
        int compactedSize = compacted.remaining();

        assertNotSame(page, compacted);
        assertEquals(0, page.position());
        assertEquals(pageSize, page.limit());
        assertTrue(compactedSize < pageSize);
        assertEquals(pageId, PageIO.getPageId(compacted));

        ByteBuffer decompress = allocateDirectBuffer(pageSize);
        decompress.put(compacted);
        decompress.flip();

        p.decompressPage(decompress);

        assertEquals(0, decompress.position());
        assertEquals(pageSize, decompress.limit());

        return decompress;
    }
}
