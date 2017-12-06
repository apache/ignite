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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import com.google.common.base.Strings;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.OpenFlags;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.file.DirectRandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.IgniteDirectIo;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.PAGE_ID_OFF;

public class FilePageStoreTest {

    @Test
    public void nativeCreateFile() throws IOException {
        File file = new File("store2.dat");
        final DirectIoLib lib = DirectIoLib.getLibForPath(file.getAbsolutePath());
        int pageSize = DataStorageConfiguration.DFLT_PAGE_SIZE;
        if (pageSize < 0 || (pageSize % lib.blockSize() != 0)) {
            throw new IllegalArgumentException("The page size [" + pageSize + "] must be a multiple of the file system block size [" + lib.blockSize() + "]");
        }
        System.out.println(pageSize);

        String pathname = file.getAbsolutePath();

        //todo flags
        int flags = OpenFlags.O_DIRECT;
      //  if (readOnly) {
            //flags |= OpenFlags.O_RDONLY;
       // } else {
            flags |= OpenFlags.O_RDWR | OpenFlags.O_CREAT;
      //  }
        int fd = IgniteDirectIo.open(pathname, flags, 00644);
        if (fd < 0) {
            throw new IOException("Error opening " + pathname + ", got " + DirectIoLib.getLastError());
        }

        NativeLong blockSize = new NativeLong(lib.blockSize());
        PointerByReference pointerToPointer = new PointerByReference();

        long capacity = pageSize;
        // align memory for use with O_DIRECT
        DirectIoLib.posix_memalign(pointerToPointer, blockSize, new NativeLong(capacity));
        Pointer pointer = pointerToPointer.getValue();

        //  GridUnsafe.copyMemory(pointer);

        ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

        buf.put(("Hi There from " + getClass().getName() + ": ").getBytes());
        buf.put(new byte[buf.remaining()]);
        buf.rewind();
        long address = GridUnsafe.bufferAddress(buf);
        System.out.println("address=" + address);
        System.out.println("address % pageSize"  + address % pageSize);

        long alignedPtr = Pointer.nativeValue(pointer);
        System.out.println("alignedPtr=" +alignedPtr);
        System.out.println("alignedPtr % pageSize=" + alignedPtr % pageSize);
        GridUnsafe.copyMemory(address, alignedPtr, pageSize);


        final int start = buf.position();
        System.out.println("start=" +start);
        assert start == lib.blockStart(start);
        final int toWrite = lib.blockEnd(buf.limit()) - start;

        System.out.println("toWrite="+ toWrite);

        NativeLong n = IgniteDirectIo.pwrite(fd, pointer, new NativeLong(pageSize), new NativeLong(pageSize*4));
        System.out.println("written=" + n);
        if (n.longValue() < 0) {
            throw new IOException("Error writing file at offset "  + ": " + DirectIoLib.getLastError());
        }

        IgniteDirectIo.close(fd);

        GridUnsafe.freeBuffer(buf);


        DirectIoLib.free(pointer);

        System.out.println("fd=" + fd);


    }

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
