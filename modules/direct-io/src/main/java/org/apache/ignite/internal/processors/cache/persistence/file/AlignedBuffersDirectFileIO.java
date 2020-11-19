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

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.processors.compress.FileSystemUtils;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;

/**
 * Limited capabilities Direct IO, which enables file write and read using aligned buffers and O_DIRECT mode.
 *
 * Works only for Linux
 */
public class AlignedBuffersDirectFileIO extends AbstractFileIO {
    /** Negative value for file offset: read/write starting from current file position */
    private static final int FILE_POS_USE_CURRENT = -1;

    /** Minimal amount of data can be written using DirectIO. */
    private final int ioBlockSize;

    /** Durable memory Page size. Can have greater value than {@link #ioBlockSize}. */
    private final int pageSize;

    /** File system block size. */
    private final int fsBlockSize;

    /** File. */
    private final File file;

    /** Logger. */
    private final IgniteLogger log;

    /** Thread local with buffers with capacity = one page {@link #pageSize} and aligned using {@link #ioBlockSize}. */
    private ThreadLocal<ByteBuffer> tlbOnePageAligned;

    /** Managed aligned buffers. Used to check if buffer is applicable for direct IO our data should be copied. */
    private ConcurrentHashMap<Long, Thread> managedAlignedBuffers;

    /** File descriptor. */
    private int fd = -1;

    /** Number of instances to cache */
    private static final int CACHED_LONGS = 512;

    /** Value used as divisor in {@link #nativeLongCache}. Native longs divisible by this value will be cached. */
    private static final int NL_CACHE_DIVISOR = 4096;

    /** Native long instance cache. */
    private static final AtomicReferenceArray<NativeLong> nativeLongCache = new AtomicReferenceArray<>(CACHED_LONGS);

    /**
     * Creates Direct File IO.
     *
     * @param ioBlockSize FS/OS block size.
     * @param pageSize Durable memory Page size.
     * @param file File to open.
     * @param modes Open options (flags).
     * @param tlbOnePageAligned Thread local with buffers with capacity = one page {@code pageSize} and aligned using
     * {@code ioBlockSize}.
     * @param managedAlignedBuffers Managed aligned buffers map, used to check if buffer is known.
     * @param log Logger.
     * @throws IOException if file open failed.
     */
    AlignedBuffersDirectFileIO(
        int ioBlockSize,
        int pageSize,
        File file,
        OpenOption[] modes,
        ThreadLocal<ByteBuffer> tlbOnePageAligned,
        ConcurrentHashMap<Long, Thread> managedAlignedBuffers,
        IgniteLogger log)
        throws IOException {
        this.log = log;
        this.ioBlockSize = ioBlockSize;
        this.pageSize = pageSize;
        this.file = file;
        this.tlbOnePageAligned = tlbOnePageAligned;
        this.managedAlignedBuffers = managedAlignedBuffers;

        String pathname = file.getAbsolutePath();

        int openFlags = setupOpenFlags(modes, log, true);
        int mode = IgniteNativeIoLib.DEFAULT_OPEN_MODE;
        int fd = IgniteNativeIoLib.open(pathname, openFlags, mode);

        if (fd < 0) {
            int error = Native.getLastError();
            String msg = "Error opening file [" + pathname + "] with flags [0x"
                + String.format("%2X", openFlags) + ": DIRECT & " + Arrays.asList(modes)
                + "], got error [" + error + ": " + getLastError() + "]";

            if (error == IgniteNativeIoLib.E_INVAL) {
                openFlags = setupOpenFlags(modes, log, false);
                fd = IgniteNativeIoLib.open(pathname, openFlags, mode);

                if (fd > 0) {
                    U.warn(log, "Disable Direct IO mode for path " + file.getParentFile() +
                        "(probably incompatible file system selected, for example, tmpfs): " + msg);

                    this.fd = fd;
                    fsBlockSize = FileSystemUtils.getFileSystemBlockSize(fd);

                    return;
                }
            }

            throw new IOException(msg);
        }

        this.fd = fd;
        fsBlockSize = FileSystemUtils.getFileSystemBlockSize(fd);
    }

    /**
     * Convert Java open options to native flags.
     *
     * @param modes java options.
     * @param log logger.
     * @param enableDirect flag for enabling option {@link IgniteNativeIoLib#O_DIRECT} .
     * @return native flags for open method.
     */
    private static int setupOpenFlags(OpenOption[] modes, IgniteLogger log, boolean enableDirect) {
        int flags = enableDirect ? IgniteNativeIoLib.O_DIRECT : 0;
        List<OpenOption> openOptionList = Arrays.asList(modes);

        for (OpenOption mode : openOptionList) {
            if (mode == StandardOpenOption.READ) {
                flags |= openOptionList.contains(StandardOpenOption.WRITE)
                    ? IgniteNativeIoLib.O_RDWR
                    : IgniteNativeIoLib.O_RDONLY;
            }
            else if (mode == StandardOpenOption.WRITE) {
                flags |= openOptionList.contains(StandardOpenOption.READ)
                    ? IgniteNativeIoLib.O_RDWR
                    : IgniteNativeIoLib.O_WRONLY;
            }
            else if (mode == StandardOpenOption.CREATE)
                flags |= IgniteNativeIoLib.O_CREAT;
            else if (mode == StandardOpenOption.TRUNCATE_EXISTING)
                flags |= IgniteNativeIoLib.O_TRUNC;
            else if (mode == StandardOpenOption.SYNC)
                flags |= IgniteNativeIoLib.O_SYNC;
            else
                log.error("Unsupported open option [" + mode + "]");
        }

        return flags;
    }

    /** {@inheritDoc} */
    @Override public int getFileSystemBlockSize() {
        return fsBlockSize;
    }

    /** {@inheritDoc} */
    @Override public long getSparseSize() {
        return FileSystemUtils.getSparseFileSize(fd);
    }

    /** {@inheritDoc} */
    @Override public int punchHole(long position, int len) {
        return (int)FileSystemUtils.punchHole(fd, position, len, fsBlockSize);
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        long position = IgniteNativeIoLib.lseek(fdCheckOpened(), 0, IgniteNativeIoLib.SEEK_CUR);

        if (position < 0) {
            throw new IOException(String.format("Error checking file [%s] position: %s",
                file, getLastError()));
        }

        return position;
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        if (IgniteNativeIoLib.lseek(fdCheckOpened(), newPosition, IgniteNativeIoLib.SEEK_SET) < 0) {
            throw new IOException(String.format("Error setting file [%s] position to [%s]: %s",
                file, Long.toString(newPosition), getLastError()));
        }
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf) throws IOException {
        return read(destBuf, FILE_POS_USE_CURRENT);
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf, long filePosition) throws IOException {
        int size = checkSizeIsPadded(destBuf.remaining());

        return isKnownAligned(destBuf) ?
            readIntoAlignedBuffer(destBuf, filePosition) :
            readIntoUnalignedBuffer(destBuf, filePosition, size);
    }

    /**
     * @param destBuf Destination aligned byte buffer.
     * @param filePosition File position.
     * @param size Buffer size to write, should be divisible by {@link #ioBlockSize}.
     * @return Number of read bytes, possibly zero, or <tt>-1</tt> if the
     *         given position is greater than or equal to the file's current size.
     * @throws IOException If failed.
     */
    private int readIntoUnalignedBuffer(ByteBuffer destBuf, long filePosition, int size) throws IOException {
        boolean useTlb = size == pageSize;
        ByteBuffer alignedBuf = useTlb ? tlbOnePageAligned.get() : AlignedBuffers.allocate(ioBlockSize, size);

        try {
            assert alignedBuf.position() == 0 : "Temporary aligned buffer is in incorrect state: position is set incorrectly";
            assert alignedBuf.limit() == size : "Temporary aligned buffer is in incorrect state: limit is set incorrectly";

            int loaded = readIntoAlignedBuffer(alignedBuf, filePosition);

            alignedBuf.flip();

            if (loaded > 0)
                  destBuf.put(alignedBuf);

            return loaded;
        }
        finally {
            alignedBuf.clear();

            if (!useTlb)
                AlignedBuffers.free(alignedBuf);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int len) throws IOException {
        return read(ByteBuffer.wrap(buf, off, len));
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        return write(srcBuf, FILE_POS_USE_CURRENT);
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long filePosition) throws IOException {
        return isKnownAligned(srcBuf) ?
            writeFromAlignedBuffer(srcBuf, filePosition) :
            writeFromUnalignedBuffer(srcBuf, filePosition);
    }

    /**
     * @param srcBuf buffer to check if it is known buffer.
     * @param filePosition File position.
     * @return Number of written bytes.
     * @throws IOException If failed.
     */
    private int writeFromUnalignedBuffer(ByteBuffer srcBuf, long filePosition) throws IOException {
        int size = srcBuf.remaining();

        boolean useTlb = size <= pageSize;
        ByteBuffer alignedBuf = useTlb ? tlbOnePageAligned.get() : AlignedBuffers.allocate(ioBlockSize, size);

        try {
            assert alignedBuf.position() == 0 : "Temporary aligned buffer is in incorrect state: position is set incorrectly";
            assert alignedBuf.limit() >= size : "Temporary aligned buffer is in incorrect state: limit is set incorrectly";

            int initPos = srcBuf.position();

            alignedBuf.put(srcBuf);
            alignedBuf.flip();

            int len = alignedBuf.remaining();

            // Compressed buffer of wrong size can be passed here.
            if (len % ioBlockSize != 0)
                alignBufferLimit(alignedBuf);

            int written = writeFromAlignedBuffer(alignedBuf, filePosition);

            // Actual written length can be greater than the original buffer,
            // since we artificially expanded it to have correctly aligned size.
            if (written > len)
                written = len;

            srcBuf.position(initPos + written);

            return written;
        }
        finally {
            alignedBuf.clear();

            if (!useTlb)
                AlignedBuffers.free(alignedBuf);
        }
    }

    /**
     * @param buf Byte buffer to align.
     */
    private void alignBufferLimit(ByteBuffer buf) {
        int len = buf.remaining();

        int alignedLen = (len / ioBlockSize + 1) * ioBlockSize;

        buf.limit(buf.limit() + alignedLen - len);
    }

    /**
     * Checks if we can run fast path: we got well known buffer is already aligned.
     *
     * @param srcBuf buffer to check if it is known buffer.
     * @return {@code true} if this buffer was allocated with alignment, may be used directly.
     */
    private boolean isKnownAligned(ByteBuffer srcBuf) {
        return srcBuf.isDirect()
            && managedAlignedBuffers != null
            && managedAlignedBuffers.containsKey(GridUnsafe.bufferAddress(srcBuf));
    }

    /**
     * Check if size is appropriate for aligned/direct IO.
     *
     * @param size buffer size to write, should be divisible by {@link #ioBlockSize}.
     * @return size from parameter.
     * @throws IOException if provided size can't be written using direct IO.
     */
    private int checkSizeIsPadded(int size) throws IOException {
        if (size % ioBlockSize != 0) {
            throw new IOException(
                String.format("Unable to apply DirectIO for read/write buffer [%d] bytes on " +
                    "block size [%d]. Consider setting %s.setPageSize(%d) or disable Direct IO.",
                    size, ioBlockSize, DataStorageConfiguration.class.getSimpleName(), ioBlockSize));
        }

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
            throw new IOException(String.format("Error %s not opened", file));

        return fd;
    }

    /**
     * Read bytes from file using Native IO and aligned buffers.
     *
     * @param destBuf Destination aligned byte buffer.
     * @param filePos Starting position of file. Providing {@link #FILE_POS_USE_CURRENT} means it is required
     *     to read from current file position.
     * @return number of bytes read from file, or <tt>-1</tt> if tried to read past EOF for file.
     * @throws IOException if reading failed.
     */
    private int readIntoAlignedBuffer(ByteBuffer destBuf, long filePos) throws IOException {
        int pos = destBuf.position();
        int limit = destBuf.limit();
        int toRead = pos <= limit ? limit - pos : 0;

        if (toRead == 0)
            return 0;

        if ((pos + toRead) > destBuf.capacity())
            throw new BufferOverflowException();

        int rd;
        Pointer ptr = bufferPtrAtPosition(destBuf, pos);

        if (filePos == FILE_POS_USE_CURRENT)
            rd = IgniteNativeIoLib.read(fdCheckOpened(), ptr, nl(toRead)).intValue();
        else
            rd = IgniteNativeIoLib.pread(fdCheckOpened(), ptr, nl(toRead), nl(filePos)).intValue();

        if (rd == 0)
            return -1; //Tried to read past EOF for file

        if (rd < 0)
            throw new IOException(String.format("Error during reading file [%s] from position [%s] : %s",
                file, filePos == FILE_POS_USE_CURRENT ? "current" : Long.toString(filePos), getLastError()));

        destBuf.position(pos + rd);

        return rd;
    }

    /**
     * Writes the aligned native buffer starting at {@code buf} to the file at offset
     * {@code filePos}. The file offset is not changed.
     *
     * @param srcBuf pointer to buffer.
     * @param filePos position in file to write data. Providing {@link #FILE_POS_USE_CURRENT} means it is required
     *     to read from current file position.
     * @return the number of bytes written.
     */
    private int writeFromAlignedBuffer(ByteBuffer srcBuf, long filePos) throws IOException {
        int pos = srcBuf.position();
        int limit = srcBuf.limit();
        int toWrite = pos <= limit ? limit - pos : 0;

        if (toWrite == 0)
            return 0;

        int wr;
        Pointer ptr = bufferPtrAtPosition(srcBuf, pos);

        if (filePos == FILE_POS_USE_CURRENT)
            wr = IgniteNativeIoLib.write(fdCheckOpened(), ptr, nl(toWrite)).intValue();
        else
            wr = IgniteNativeIoLib.pwrite(fdCheckOpened(), ptr, nl(toWrite), nl(filePos)).intValue();

        if (wr < 0) {
            throw new IOException(String.format("Error during writing file [%s] to position [%s]: %s",
                file, filePos == FILE_POS_USE_CURRENT ? "current" : Long.toString(filePos), getLastError()));
        }

        if ((pos + wr) > limit) {
            throw new IllegalStateException(String.format("Write illegal state for file [%s]: pos=%d, wr=%d, limit=%d",
                file, pos, wr, limit));
        }

        srcBuf.position(pos + wr);

        return wr;
    }

    /**
     * @param val value to box to native long.
     * @return native long.
     */
    @NotNull private static NativeLong nl(long val) {
        if (val % NL_CACHE_DIVISOR == 0 && val < CACHED_LONGS * NL_CACHE_DIVISOR) {
            int cacheIdx = (int)(val / NL_CACHE_DIVISOR);

            NativeLong curCached = nativeLongCache.get(cacheIdx);

            if (curCached != null)
                return curCached;

            NativeLong nl = new NativeLong(val);

            nativeLongCache.compareAndSet(cacheIdx, null, nl);

            return nl;
        }
        return new NativeLong(val);
    }

    /**
     * Retrieve last error set by the OS as string. This corresponds to and <code>errno</code> on
     * *nix platforms.
     * @return displayable string with OS error info.
     */
    private static String getLastError() {
        return IgniteNativeIoLib.strerror(Native.getLastError());
    }

    /**
     * Gets address in memory for direct aligned buffer taking into account its current {@code position()} as offset.
     * Produces warnings if data or offset seems to be not aligned.
     *
     * @param buf Direct aligned buffer.
     * @param pos position, used as offset for resulting pointer.
     * @return Buffer memory address.
     */
    @NotNull private Pointer bufferPtrAtPosition(ByteBuffer buf, int pos) {
        long alignedPointer = GridUnsafe.bufferAddress(buf);

        if (pos < 0)
            throw new IllegalArgumentException();

        if (pos > buf.capacity())
            throw new BufferOverflowException();

        if ((alignedPointer + pos) % ioBlockSize != 0) {
            U.warn(log, String.format("IO Buffer Pointer [%d] and/or offset [%d] seems to be not aligned " +
                "for IO block size [%d]. Direct IO may fail.", alignedPointer, buf.position(), ioBlockSize));
        }

        return new Pointer(alignedPointer + pos);
    }

    /** {@inheritDoc} */
    @Override public int write(byte[] buf, int off, int len) throws IOException {
        return write(ByteBuffer.wrap(buf, off, len));
    }

    /** {@inheritDoc} */
    @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
        throw new UnsupportedOperationException("AsynchronousFileChannel doesn't support mmap.");
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        force(false);
    }

    /** {@inheritDoc} */
    @Override public void force(boolean withMetadata) throws IOException {
        int fd = fdCheckOpened();

        int res = withMetadata ? IgniteNativeIoLib.fsync(fd) : IgniteNativeIoLib.fdatasync(fd);

        if (res < 0)
            throw new IOException(String.format("Error fsync()'ing %s, got %s", file, getLastError()));
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        return file.length();
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IOException {
        truncate(0);
    }

    /**
     * Truncates this channel's file to the given size.
     *
     * <p> If the given size is less than the file's current size then the file
     * is truncated, discarding any bytes beyond the new end of the file. If
     * the given size is greater than or equal to the file's current size then
     * the file is not modified. In either case, if this channel's file
     * position is greater than the given size then it is set to that size.
     * </p>
     *
     * @param  size The new size, a non-negative byte count
     *
     */
    private void truncate(long size) throws IOException {
        if (IgniteNativeIoLib.ftruncate(fdCheckOpened(), size) < 0)
            throw new IOException(String.format("Error truncating file %s, got %s", file, getLastError()));

        if (position() > size)
            position(size);
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (IgniteNativeIoLib.close(fdCheckOpened()) < 0)
            throw new IOException(String.format("Error closing %s, got %s", file, getLastError()));

        fd = -1;
    }
}
