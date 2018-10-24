package org.apache.ignite.internal.processors.compress;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.PageCompression.DROP_GARBAGE;

/**
 */
public class CompressionProcessorTest extends GridCommonAbstractTest {
    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testProcessor() throws IgniteCheckedException {
        final Random rnd = ThreadLocalRandom.current();

        final int blockSize = 1024;
        final int pageSize = 4 * 1024;
        final long pageId = 0xABCDE9876B8A7D6AL;

        final byte[][] rows = new byte[][]{
            new byte[29],
            new byte[57],
            new byte[91]
        };

        for (byte[] row : rows)
            rnd.nextBytes(row);

        ByteBuffer page = ByteBuffer.allocateDirect(pageSize * 3);
        page.position(pageSize);
        page.limit(pageSize * 2);

        long pageAddr = GridUnsafe.bufferAddress(page) + pageSize;

        SimpleDataPageIO io = SimpleDataPageIO.VERSIONS.latest();

        io.initNewPage(pageAddr, pageId, pageSize);

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
        assertEquals(pageSize, page.position());
        assertEquals(2 * pageSize, page.limit());
        assertTrue(compactedSize < pageSize);

        ByteBuffer decompress = ByteBuffer.allocateDirect(pageSize * 3);
        decompress.position(pageSize);
        decompress.put(compacted);
        decompress.flip();
        decompress.position(pageSize);

        p.decompressPage(decompress);

        assertEquals(pageSize, decompress.position());
        assertEquals(pageSize * 2, decompress.limit());

        long pageAddrx = GridUnsafe.bufferAddress(decompress) + pageSize;

        assertSame(io, PageIO.getPageIO(pageAddrx));
        assertEqualsCollections(links, io.forAllItems(pageAddrx, (link) -> link));
    }
}
