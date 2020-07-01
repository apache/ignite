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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Native IO library based on *nix C library, enabled for Linux, kernel version >= 2.4.10. <br>
 * <br>
 * Uses JNA library (https://github.com/java-native-access/jna) to access native calls. <br>
 * <br>
 */
@SuppressWarnings({"OctalInteger", "WeakerAccess"})
public class IgniteNativeIoLib {
    /** Open for reading only. */
    public static final int O_RDONLY = 00;

    /** Open for writing only. */
    public static final int O_WRONLY = 01;

    /** Open for reading and writing. */
    public static final int O_RDWR = 02;

    /** File shall be created. If the file exists, this flag has no effect. */
    public static final int O_CREAT = 0100;

    /** If the file exists and is a regular file length shall be truncated to 0. */
    public static final int O_TRUNC = 01000;

    /** Try to minimize cache effects of the I/O to and from this file.  */
    public static final int O_DIRECT = 040000;

    /**
     * Write operations on the file will complete according to the requirements of synchronized I/O file integrity
     * completion. By the time write(2) (or similar) returns, the output data and associated file metadata have been
     * transferred to the underlying hardware.
     */
    public static final int O_SYNC = 04000000;

    /**
     * The specified data will not be accessed in the near future. See fadvise.h and "man 2 posix_fadvise".
    */
    public static final int POSIX_FADV_DONTNEED = 4;

    /** Flag for newly created files: user has read permission. */
    public static final int S_IRUSR = 00400;

    /** Flag for newly created files: user has write permission. */
    public static final int S_IWUSR = 00200;

    /** Flag for newly created files: group has read permission. */
    public static final int S_IRGRP = 00040;

    /** Flag for newly created files: others have read permission. */
    public static final int S_IROTH = 00004;

    /** Default access mask for newly created files. */
    public static final int DEFAULT_OPEN_MODE = S_IRUSR | S_IWUSR | S_IROTH | S_IRGRP;

    /** Invalid argument. */
    public static final int E_INVAL = 22;

    /** Seek option: set file offset to offset */
    public static final int SEEK_SET = 0;

    /** Seek option: change file position to offset */
    public static final int SEEK_CUR = 1;

    /** JNA library available and initialized. Always {@code false} for non linux systems. */
    private static boolean jnaAvailable;

    /** JNA library initialization exception. To be logged to Ignite logger later. */
    @Nullable private static Exception ex;

    static {
        if (Platform.isLinux()) {
            try {
                if (checkLinuxVersion()) {
                    Native.register(Platform.C_LIBRARY_NAME);
                    jnaAvailable = true;
                }
                else
                    jnaAvailable = false;
            }
            catch (Exception e) {
                ex = e;
                jnaAvailable = false;
            }
        }
        else
            jnaAvailable = false;
    }

    /**
     * O_DIRECT  support was added under Linux in kernel version 2.4.10.
     *
     * @return {@code true} if O_DIRECT is supported, kernel version >= 2.4.10
     */
    private static boolean checkLinuxVersion() {
        final String osVer = System.getProperty("os.version");

        if (osVer == null)
            return false;

        List<Integer> verIntComps = new ArrayList<>();

        for (StringTokenizer tokenizer = new StringTokenizer(osVer, ".-"); tokenizer.hasMoreTokens(); ) {
            String verComp = tokenizer.nextToken();

            if (verComp.matches("\\d*"))
                verIntComps.add(Integer.parseInt(verComp));
        }

        if (verIntComps.isEmpty())
            return false;

        final int verIdx = 0;
        final int majorRevIdx = 1;
        final int minorRevIdx = 2;

        if (verIntComps.get(verIdx) > 2)
            return true;
        else if (verIntComps.get(verIdx) == 2) {
            int compsCnt = verIntComps.size();

            if (compsCnt > majorRevIdx && verIntComps.get(majorRevIdx) > 4)
                return true;
            else if (compsCnt > minorRevIdx
                && verIntComps.get(majorRevIdx) == 4
                && verIntComps.get(minorRevIdx) >= 10)
                return true;
        }
        return false;
    }

    /**
     * Calculate Lowest Common Multiplier.
     * @param a first value.
     * @param b second value.
     */
    private static long lcm(final long a, final long b) {
        return (a * b) / gcf(a, b);
    }

    /**
     * Calculate Greatest Common Factor.
     * @param a first value.
     * @param b second value.
     */
    private static long gcf(final long a, final long b) {
        if (b == 0)
            return a;
        else
            return gcf(b, a % b);
    }

    /**
     * Determines FS and OS block size. Returns file system block size for use with storageDir see "man 3 posix_memalign"
     *
     * @param storageDir storage path, base path to check (FS) configuration parameters.
     * @param log Logger.
     * @return <ul><li>FS block size to be used in Direct IO and memory alignments.</li>
     * <li>or <tt>-1</tt> Operating System is not applicable for enabling Direct IO.</li>
     * <li>and <tt>-1</tt> if failed to determine block size.</li>
     * <li>and <tt>-1</tt> if JNA is not available or init failed.</li> </ul>
     */
    public static int getDirectIOBlockSize(final String storageDir, final IgniteLogger log) {
        if (ex != null) {
            U.warn(log, "Failed to initialize O_DIRECT support at current OS: " + ex.getMessage(), ex);

            return -1;
        }

        if (!jnaAvailable)
            return -1;

        int fsBlockSize = -1;
        int _PC_REC_XFER_ALIGN = 0x11;
        int pcAlign = pathconf(storageDir, _PC_REC_XFER_ALIGN).intValue();

        if (pcAlign > 0)
            fsBlockSize = pcAlign;

        int pageSize = getpagesize();

        fsBlockSize = (int)lcm(fsBlockSize, pageSize);

        // just being completely paranoid: (512 is the rule for 2.6+ kernels)
        fsBlockSize = (int)lcm(fsBlockSize, 512);

        if (log.isInfoEnabled())
            log.info(String.format("Page size configuration for storage path [%s]: %d;" +
                    " Linux memory page size: %d;" +
                    " Selected FS block size : %d.",
                storageDir, pcAlign, pageSize, fsBlockSize));

        // lastly, a sanity check
        if (fsBlockSize <= 0 || ((fsBlockSize & (fsBlockSize - 1)) != 0)) {
            U.warn(log, "File system block size should be a power of two, was found to be " + fsBlockSize +
                " Disabling O_DIRECT support");

            return -1;
        }

        if (log.isInfoEnabled())
            log.info("Selected FS block size : " + fsBlockSize);

        return fsBlockSize;
    }

    /**
     * @return Flag indicating JNA library available and initialized. Always {@code false} for non linux systems.
     */
    public static boolean isJnaAvailable() {
        return jnaAvailable;
    }

    /**
     * Open a file. See "man 3 open".
     *
     * @param pathname pathname naming the file.
     * @param flags flag/open options. Flags are constructed by a bitwise-inclusive OR of flags.
     * @param mode create file mode creation mask.
     * @return file descriptor.
     */
    public static native int open(String pathname, int flags, int mode);

    /**
     * See "man 2 close".
     *
     * @param fd The file descriptor of the file to close.
     * @return 0 on success, -1 on error.
     */
    public static native int close(int fd);

    /**
     * Writes up to {@code cnt} bytes to the buffer starting at {@code buf} to the file descriptor {@code fd} at offset
     * {@code offset}. The file offset is not changed. See "man 2 pwrite".
     *
     * @param fd file descriptor.
     * @param buf pointer to buffer with data.
     * @param cnt bytes to write.
     * @param off position in file to write data.
     * @return the number of bytes written. Note that is not an error for a successful call to transfer fewer bytes than
     * requested.
     */
    public static native NativeLong pwrite(int fd, Pointer buf, NativeLong cnt, NativeLong off);

    /**
     * Writes up to {@code cnt} bytes to the buffer starting at {@code buf} to the file descriptor {@code fd}.
     * The file offset is changed. See "man 2 write".
     *
     * @param fd file descriptor.
     * @param buf pointer to buffer with data.
     * @param cnt bytes to write.
     * @return the number of bytes written. Note that is not an error for a successful call to transfer fewer bytes than
     * requested.
     */
    public static native NativeLong write(int fd, Pointer buf, NativeLong cnt);

    /**
     * Reads up to {@code cnt} bytes from file descriptor {@code fd} at offset {@code off} (from the start of the file)
     * into the buffer starting at {@code buf}. The file offset is not changed. See "man 2 pread".
     *
     * @param fd file descriptor.
     * @param buf pointer to buffer to place the data.
     * @param cnt bytes to read.
     * @return On success, the number of bytes read is returned (zero indicates end of file), on error, -1 is returned,
     * and errno is set appropriately.
     */
    public static native NativeLong pread(int fd, Pointer buf, NativeLong cnt, NativeLong off);

    /**
     * Reads up to {@code cnt} bytes from file descriptor {@code fd} into the buffer starting at {@code buf}. The file
     * offset is changed. See "man 2 read".
     *
     * @param fd file descriptor.
     * @param buf pointer to buffer to place the data.
     * @param cnt bytes to read.
     * @return On success, the number of bytes read is returned (zero indicates end of file), on error, -1 is returned,
     * and errno is set appropriately.
     */
    public static native NativeLong read(int fd, Pointer buf, NativeLong cnt);

    /**
     * Synchronize a file's in-core state with storage device. See "man 2 fsync".
     * @param fd file descriptor.
     * @return On success return zero. On error, -1 is returned, and errno is set appropriately.
     */
    public static native int fsync(int fd);

    /**
     * Synchronize a file's in-core state with storage device. See "man 2 fsync".
     *
     * Similar to {@link #fsync(int)}, but does not flush modified metadata unless that metadata is needed in order to allow a subsequent data retrieval to be correctly handled
     *
     * @param fd file descriptor.
     * @return On success return zero. On error, -1 is returned, and errno is set appropriately.
     */
    public static native int fdatasync(int fd);

    /**
     * Allocates size bytes and places the address of the allocated memory in {@code memptr}.
     * The address of the allocated memory will be a multiple of {@code alignment}.
     *
     * See "man 3 posix_memalign".
     * @param memptr out memory pointer.
     * @param alignment memory alignment,  must be a power of two and a multiple of sizeof(void *).
     * @param size size of buffer.
     * @return returns zero on success, or one of the error values.
     */
    public static native int posix_memalign(PointerByReference memptr, NativeLong alignment, NativeLong size);

    /**
     * Frees the memory space pointed to by ptr, which must have been returned by a previous call to native allocation
     * methods. POSIX requires that memory obtained from {@link #posix_memalign} can be freed using free. See "man 3
     * free".
     *
     * @param ptr pointer to free.
     */
    public static native void free(Pointer ptr);

    /**
     * Function returns a string that describes the error code passed in the argument {@code errnum}. See "man 3
     * strerror".
     *
     * @param errnum error code.
     * @return displayable error information.
     */
    public static native String strerror(int errnum);

    /**
     * Return path (FS) configuration parameter value. <br>
     * Helps to determine alignment restrictions, for example, on buffers used for direct block device I/O. <br>
     * POSIX specifies the pathconf(path,_PC_REC_XFER_ALIGN) call that tells what alignment is needed.
     *
     * @param path base path to check settings.
     * @param name variable name to query.
     */
    public static native NativeLong pathconf(String path, int name);

    /**
     * The function getpagesize() returns the number of bytes in a memory
     * page, where "page" is a fixed-length block, the unit for memory
     * allocation and file mapping
     */
    public static native int getpagesize();

    /**
     * Allows to announce an intention to access file data in a specific pattern in the future, thus allowing the
     * kernel to perform appropriate optimizations.
     *
     * The advice applies to a (not necessarily existent) region starting at
     * {@code off} and extending for {@code len} bytes (or until the end of the file if len is 0)
     * within the file referred to by fd.
     *
     * See "man 2 posix_fadvise".
     *
     * @param fd file descriptor.
     * @param off region start.
     * @param len region end.
     * @param flag advice (option) to apply.
     * @return On success, zero is returned.  On error, an error number is returned.
     */
    public static native int posix_fadvise(int fd, long off, long len, int flag);

    /**
     * Causes regular file referenced by fd to be truncated to a size of precisely length bytes.
     *
     * If the file previously was larger than this size, the extra data is lost.
     * If the file previously was shorter, it is extended, and the extended part reads as null bytes ('\0').
     * The file offset is not changed.
     *
     * @param fd  file descriptor.
     * @param len required length.
     * @return On success, zero is returned. On error, -1 is returned, and errno is set appropriately.
     */
    public static native int ftruncate(int fd, long len);

    /**
     * Repositions the file offset of the open file description associated with the file descriptor {@code fd}
     * to the argument offset according to the directive {@code whence}
     * @param fd file descriptor.
     * @param off required position offset.
     * @param whence position base.
     * @return  On error, the value -1 is returned and errno is set to indicate the error.
     */
    public static native long lseek(int fd, long off, int whence);
}
