package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import com.google.common.base.Strings;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.file.DirectRandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.PAGE_ID_OFF;

public class FilePageStoreTest {

    @Test
    public void saveRead() throws IgniteCheckedException {
        RandomAccessFileIOFactory ioFactory = new RandomAccessFileIOFactory();
        testSaveRead(ioFactory);

    }


    @Test
    public void saveReadDirect() throws IgniteCheckedException {
        testSaveRead(new DirectRandomAccessFileIOFactory());
    }


    private void testSaveRead(FileIOFactory ioFactory) throws IgniteCheckedException {
        int pageSize = DataStorageConfiguration.DFLT_PAGE_SIZE;
        DataStorageConfiguration cfg = new DataStorageConfiguration();
        cfg.setPageSize(pageSize);

        FileVersionCheckingFactory factory = new FileVersionCheckingFactory(ioFactory, cfg);
        File file = new File("store.dat");
        System.err.println(file.getAbsolutePath());

        FilePageStore store = factory.createPageStore((byte)0, file, 2);
        for (int i = 0; i < 100; i++) {
            long l = store.allocatePage();
            System.err.println(l);

            ByteBuffer order = dataBuf(pageSize);

            store.write(l, order, 1);
        }
        store.sync();
    }

    @NotNull private ByteBuffer dataBuf(int pageSize) {
        ByteBuffer data = ByteBuffer.wrap(Strings.repeat("D", pageSize- PAGE_ID_OFF).getBytes());
        ByteBuffer order = ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
        order.put(new byte[PAGE_ID_OFF]);
        order.put(data.array());
        order.rewind();
        return order;
    }

}
