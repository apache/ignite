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

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.NotNull;
import org.jsr166.ConcurrentHashMap8;
import sun.nio.ch.DirectBuffer;

/**
 * Limited capabilities Direct IO, which enables file write and read using aligned buffers and O_DIRECT mode.
 *
 * Works only for Linux
 */
public class AlignedBuffersDirectFileIO implements FileIO {

    private final int fsBlockSize;
    private final int pageSize;
    private final File file;

    private ThreadLocal<ByteBuffer> tblOnePageAligned;
    private ConcurrentHashMap8<Long, String> managedAlignedBuffers;

    private int fd = -1;

    public AlignedBuffersDirectFileIO(
        final int fsBlockSize,
        final int pageSize,
        final File file,
        final OpenOption[] modes,
        final ThreadLocal<ByteBuffer> tblOnePageAligned,
        final ConcurrentHashMap8<Long, String> managedAlignedBuffers) throws IOException {

        this.fsBlockSize = fsBlockSize;
        this.pageSize = pageSize;
        this.file = file;
        this.tblOnePageAligned = tblOnePageAligned;
        this.managedAlignedBuffers = managedAlignedBuffers;

        //todo flags from modes
        int flags = IgniteNativeIoLib.O_DIRECT;
        //  if (readOnly) {
        //flags |= OpenFlags.O_RDONLY;
        // } else {
        flags |= IgniteNativeIoLib.O_RDWR | IgniteNativeIoLib.O_CREAT;
        //  }
        final String pathname = file.getAbsolutePath();

        final int fd = IgniteNativeIoLib.open(pathname, flags, 00644);

        if (fd < 0)
            throw new IOException("Error opening file [" + pathname + "], got error [" + getLastError() + "]");

        this.fd = fd;
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf, long position) throws IOException {
        int size = checkSizeIsPadded(destBuf.remaining());

        fdCheckOpened();

        //fast path, well known buffer, already aligned
        if (isAligned(destBuf)) {
            int loaded = readAligned(destBuf, position);
            if (loaded < 0)
                return loaded;

            return moveBufPosition(destBuf, loaded);
        }

        final boolean useTlb = size == pageSize;
        final ByteBuffer alignedBuf = useTlb ? tblOnePageAligned.get() : AlignedBuffer.allocate(fsBlockSize, size);

        try {

            final int loaded = readAligned(alignedBuf, position);
            if (loaded < 0)
                return loaded;

            AlignedBuffer.copyMemory(alignedBuf, destBuf);

            return moveBufPosition(destBuf, loaded);
        }
        finally {
            if (!useTlb)
                AlignedBuffer.free(alignedBuf);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        int size = checkSizeIsPadded(srcBuf.remaining());

        fdCheckOpened();

        if (isAligned(srcBuf)) {
            int written = writeAligned(srcBuf);

            return moveBufPosition(srcBuf, written);
        }

        final boolean useTlb = size == pageSize;
        final ByteBuffer alignedBuf = useTlb ? tblOnePageAligned.get() : AlignedBuffer.allocate(fsBlockSize, size);
        try {
            AlignedBuffer.copyMemory(srcBuf, alignedBuf);

            final int written = writeAligned(alignedBuf);

            return moveBufPosition(srcBuf, written);
        }
        finally {
            if (!useTlb)
                AlignedBuffer.free(alignedBuf);
        }
    }

    /** {@inheritDoc} */
    @Override public int write(final ByteBuffer srcBuf, final long position) throws IOException {
        int size = checkSizeIsPadded(srcBuf.remaining());

        if (isAligned(srcBuf)) {
            int written = writeAligned(srcBuf, position);

            return moveBufPosition(srcBuf, written);
        }

        final boolean useTlb = size == pageSize;
        final ByteBuffer alignedBuf = useTlb ? tblOnePageAligned.get() : AlignedBuffer.allocate(fsBlockSize, size);
        try {
            AlignedBuffer.copyMemory(srcBuf, alignedBuf);

            final int written = writeAligned(alignedBuf, position);

            return moveBufPosition(srcBuf, written);
        }
        finally {
            if (!useTlb)
                AlignedBuffer.free(alignedBuf);
        }
    }

    /**
     * @param srcBuf
     * @param bytes
     * @return
     */
    private int moveBufPosition(ByteBuffer srcBuf, int bytes) {
        srcBuf.position(srcBuf.position() + bytes);

        return bytes;
    }

    /**
     * @param srcBuf buffer to check if it is known buffer
     * @return true if this buffer was allocated with alignment, may be used directly
     */
    private boolean isAligned(ByteBuffer srcBuf) {
        return srcBuf instanceof DirectBuffer
            && managedAlignedBuffers != null
            && managedAlignedBuffers.contains(GridUnsafe.bufferAddress(srcBuf));
    }

    /**
     * Check if size is appropriate for aligned/direct IO.
     *
     * @param size buffer size to write, should be divisible by {@link #fsBlockSize}.
     * @return size from parameter.
     * @throws IOException if provided size can't be written using direct IO.
     */
    private int checkSizeIsPadded(int size) throws IOException {
        if (size % fsBlockSize != 0)
            throw new IOException("Unable to apply DirectIO for read/write buffer [" + size + "] bytes" +
                " on file system block size [" + fsBlockSize + "]." +
                " Set " + DataStorageConfiguration.class.getSimpleName() + ".setPageSize(" + fsBlockSize + ").");

        return size;
    }

    /**
     * Checks if file is opened and returns descriptor.
     *
     * @return file descriptor.
     * @throws IOException if file not opened.
     */
    private int fdCheckOpened() throws IOException {
        if (fd < 0)
            throw new IOException("Error " + file + " not opened");

        return fd;
    }

    private int readAligned(ByteBuffer buf, long position) throws IOException {
        int rd = IgniteNativeIoLib.pread(fdCheckOpened(), bufferPtr(buf), nl(buf.remaining()), nl(position)).intValue();

        if (rd == 0)
            return -1; //Tried to read past EOF for file

        if (rd < 0)
            throw new IOException("Error during reading file [" + file + "] " + ": " + getLastError());

        return rd;
    }

    private int writeAligned(ByteBuffer buf, long position) throws IOException {
        int wr = IgniteNativeIoLib.pwrite(fdCheckOpened(), bufferPtr(buf), nl(buf.remaining()), nl(position)).intValue();

        if (wr < 0)
            throw new IOException("Error during writing file [" + file + "] to position [" + position + "]: " + getLastError());

        return wr;
    }

    private int writeAligned(ByteBuffer buf) throws IOException {
        int wr = IgniteNativeIoLib.write(fdCheckOpened(), bufferPtr(buf), nl(buf.remaining())).intValue();

        if (wr < 0)
            throw new IOException("Error during writing file [" + file + "] " + ": " + getLastError());

        return wr;
    }

    @NotNull private NativeLong nl(long position) {
        return new NativeLong(position);
    }

    public static String getLastError() {
        return IgniteNativeIoLib.strerror(Native.getLastError());
    }

    @NotNull private Pointer bufferPtr(ByteBuffer buf) {
        long alignedPointer = GridUnsafe.bufferAddress(buf);

        if ((alignedPointer + buf.position()) % fsBlockSize != 0) {
            //todo warning
        }

        return new Pointer(alignedPointer);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        if (IgniteNativeIoLib.fsync(fdCheckOpened()) < 0)
            throw new IOException("Error fsync()'ing " + file + ", got " + getLastError());
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (IgniteNativeIoLib.close(fdCheckOpened()) < 0)
            throw new IOException("Error closing " + file + ", got " + getLastError());

        fd = -1;
    }
}
