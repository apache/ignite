package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/** */
public class FilePageStoreSelfTest {
    /**
     * Mock test to be sure that in case:
     * 1. {@link FilePageStore#getCrcSize(long, ByteBuffer)} < pageSize
     * 2. {@link FilePageStore#calcCrc32(ByteBuffer, int)} == 0
     * Page will be successfully written.
     * Please, see other changes from commit to fully understand the test.
     */
    @Test
    public void testCrcCalculationOnWrite() throws Exception {
        FilePageStore store = Mockito.spy(new FilePageStore(
            FLAG_DATA,
            () -> {
                try {
                    File file = File.createTempFile("pagestore", "tmp");

                    file.deleteOnExit();

                    return file.toPath();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            },
            new RandomAccessFileIOFactory(),
            DFLT_PAGE_SIZE,
            l -> {}
        ));

        ByteBuffer buf = (ByteBuffer)ByteBuffer.allocateDirect(DFLT_PAGE_SIZE)
            .order(ByteOrder.nativeOrder())
            .rewind();

        // Make CRC calculated over first 4 bytes.
        Mockito.doReturn(Integer.BYTES).when(store).getCrcSize(Mockito.anyLong(), Mockito.any());

        // First 4 FF bytes leads to CRC == 0.
        buf.put(U.intToBytes(0xFFFFFFFF));

        // Put one more byte to make full page CRC not zero.
        buf.put(1024, (byte)42);
        buf.rewind();

        assertEquals(0, FilePageStore.calcCrc32(buf, Integer.BYTES));
        assertNotEquals(0, FilePageStore.calcCrc32(buf, DFLT_PAGE_SIZE));

        buf.rewind();

        store.write(0, buf, 0, true);
    }
}
