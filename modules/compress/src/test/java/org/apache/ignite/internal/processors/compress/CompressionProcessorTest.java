package org.apache.ignite.internal.processors.compress;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.PageCompression.DROP_GARBAGE;
import static org.apache.ignite.internal.processors.compress.CompressionProcessorImpl.allocateDirectBuffer;

/**
 */
public class CompressionProcessorTest extends GridCommonAbstractTest {
    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testCompressionProcessor() throws IgniteCheckedException {
        final Random rnd = ThreadLocalRandom.current();

        final int blockSize = 1024;
        final int pageSize = 4 * 1024;

        final byte[][] rows = new byte[][]{
            new byte[29],
            new byte[57],
            new byte[91]
        };

        for (byte[] row : rows)
            rnd.nextBytes(row);

        ByteBuffer page = allocateDirectBuffer(pageSize);
        long pageAddr = GridUnsafe.bufferAddress(page);

        SimpleDataPageIO io = SimpleDataPageIO.VERSIONS.latest();

        long pageId = PageIdUtils.pageId(PageIdAllocator.MAX_PARTITION_ID, PageIdAllocator.FLAG_DATA, 171717);

        io.initNewPage(pageAddr, pageId, pageSize);

        assertSame(io, PageIO.getPageIO(pageAddr));
        assertSame(io, PageIO.getPageIO(page));

        GridIntList itemIds = new GridIntList();

        while (io.getFreeSpace(pageAddr) > 100)
            itemIds.add(io.addRow(pageAddr, rows[rnd.nextInt(rows.length)], pageSize));

        for (int i = 0; i < itemIds.size(); i += 2)
            io.removeRow(pageAddr, itemIds.get(i), pageSize);

        List<Long> links = io.forAllItems(pageAddr, (link) -> link);

        assertFalse(links.isEmpty());

        CompressionProcessorImpl p = new CompressionProcessorImpl(new GridTestKernalContext(log));

        ByteBuffer compacted = p.compressPage(page, blockSize, DROP_GARBAGE, 0);
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

        long pageAddrx = GridUnsafe.bufferAddress(decompress);

        assertSame(io, PageIO.getPageIO(decompress));
        assertSame(io, PageIO.getPageIO(pageAddrx));
        assertEquals(links, io.forAllItems(pageAddrx, (link) -> link));
    }
}
