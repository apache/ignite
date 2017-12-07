/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.file;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.OpenFlags;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Limited capabilities Direct IO, which enables file write and read using aligned buffers and O_DIRECT mode.
 *
 * Works only for Linux
 */
public class AlignedBuffersDirectFileIO implements FileIO {

    private final int fsBlockSize;
    private final File file;
    private final OpenOption[] modes;
    private int fd = -1;

    public AlignedBuffersDirectFileIO(int fsBlockSize, File file, OpenOption[] modes) throws IOException {
        this.fsBlockSize = fsBlockSize;
        this.file = file;
        this.modes = modes;

        String pathname = file.getAbsolutePath();

        //todo flags from modes
        int flags = OpenFlags.O_DIRECT;
        //  if (readOnly) {
        //flags |= OpenFlags.O_RDONLY;
        // } else {
        flags |= OpenFlags.O_RDWR | OpenFlags.O_CREAT;
        //  }
        int fd = IgniteNativeIoLib.open(pathname, flags, 00644);
        if (fd < 0) {
            throw new IOException("Error opening " + pathname + ", got " + DirectIoLib.getLastError());
        }
        this.fd = fd;
    }

    @Override public long position() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override public void position(long newPosition) throws IOException {

        throw new UnsupportedOperationException("Not implemented");
    }

    @Override public int read(ByteBuffer destinationBuffer) throws IOException {

        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destinationBuffer, long position) throws IOException {
        int size = checkSizeIsPadded(destinationBuffer.remaining());

        checkOpened();

        ByteBuffer alignedBuf = AlignedBuffer.allocate(fsBlockSize, size);
        try {

            int loaded = readAligned(alignedBuf, position);
            if (loaded < 0)
                return loaded;

            AlignedBuffer.copyMemory(alignedBuf, destinationBuffer);
            destinationBuffer.position(destinationBuffer.position() + loaded);
            return loaded;
        }
        finally {
            AlignedBuffer.free(alignedBuf);
        }
    }

    @Override public int read(byte[] buffer, int offset, int length) throws IOException {

        throw new UnsupportedOperationException("Not implemented");
    }

    @Override public int write(ByteBuffer sourceBuffer) throws IOException {
        int size = checkSizeIsPadded(sourceBuffer.remaining());

        checkOpened();

        ByteBuffer alignedBuf = AlignedBuffer.allocate(fsBlockSize, size);
        try {
            AlignedBuffer.copyMemory(sourceBuffer, alignedBuf);

            int written = writeAligned(alignedBuf);

            sourceBuffer.position(sourceBuffer.position() + written);
            return written;
        }
        finally {
            AlignedBuffer.free(alignedBuf);
        }
    }

    private int checkSizeIsPadded(int size) throws IOException {
        if (size % fsBlockSize != 0) {
            throw new IOException("Unable to apply DirectIO for read/write of buffer size [" + size + "] on page size [" + fsBlockSize + "]");
        }
        return size;
    }

    private void checkOpened() throws IOException {
        if (fd < 0) {
            throw new IOException("Error " + file + " not opened");
        }
    }

    private int readAligned(ByteBuffer buf, long position) throws IOException {
        long alignedPointer = GridUnsafe.bufferAddress(buf);

        if (alignedPointer % fsBlockSize != 0) {
            //todo warning
        }

        Pointer pointer = new Pointer(alignedPointer);
        NativeLong n = IgniteNativeIoLib.pread(fd, pointer, new NativeLong(buf.remaining()), new NativeLong(position));

        int rd = n.intValue();

        System.out.println("read=" + rd); //todo remove

        if (rd == 0) {
            System.out.println("Tried to read past EOF for [" + file + "] at offset " + position + " into ByteBuffer " + buf);
            return -1;
        }

        if (rd < 0) {
            throw new IOException("Error during reading file [" + file + "] " + ": " + DirectIoLib.getLastError());
        }
        return rd;
    }

    private int writeAligned(ByteBuffer buf) throws IOException {
        long alignedPointer = GridUnsafe.bufferAddress(buf);

        if (alignedPointer % fsBlockSize != 0) {
            //todo warning
        }

        Pointer pointer = new Pointer(alignedPointer);
        NativeLong n = IgniteNativeIoLib.write(fd, pointer, new NativeLong(buf.remaining()));
        int wr = n.intValue();
        System.out.println("written=" + wr); //todo remove
        if (wr < 0) {
            throw new IOException("Error during writing file [" + file + "] " + ": " + DirectIoLib.getLastError());
        }
        return wr;
    }

    @Override public int write(ByteBuffer sourceBuffer, long position) throws IOException {

        throw new UnsupportedOperationException("Not implemented");
    }

    @Override public void write(byte[] buffer, int offset, int length) throws IOException {

        throw new UnsupportedOperationException("Not implemented");
    }

    @Override public void force() throws IOException {

        throw new UnsupportedOperationException("Not implemented");
    }

    @Override public long size() throws IOException {

        throw new UnsupportedOperationException("Not implemented");
    }

    @Override public void clear() throws IOException {

        throw new UnsupportedOperationException("Not implemented");
    }

    @Override public void close() throws IOException {

        throw new UnsupportedOperationException("Not implemented");
    }
}
