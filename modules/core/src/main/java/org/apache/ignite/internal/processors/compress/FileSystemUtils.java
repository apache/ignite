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

package org.apache.ignite.internal.processors.compress;

import java.nio.file.Path;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Native file system API.
 */
public final class FileSystemUtils {
    /** */
    private static final String NATIVE_FS_LINUX_CLASS =
        "org.apache.ignite.internal.processors.compress.NativeFileSystemLinux";

    /** */
    private static final NativeFileSystem fs;

    /** */
    private static volatile Throwable err;

    /** */
    static {
        NativeFileSystem x = null;

        try {
            if (IgniteComponentType.COMPRESSION.inClassPath()) {
                if (U.isLinux())
                    x = U.newInstance(NATIVE_FS_LINUX_CLASS);
            }
        }
        catch (Throwable e) {
            err = e;
        }

        fs = x;
    }

    /**
     */
    public static void checkSupported() {
        Throwable e = err;

        if (e != null || fs == null)
            throw new IgniteException("Native file system API is not supported on " + U.osString(), e);
    }

    /**
     * @param path File system path.
     * @return File system block size or negative value if not supported.
     */
    public static int getFileSystemBlockSize(Path path) {
        return fs == null ? -1 : fs.getFileSystemBlockSize(path);
    }

    /**
     * @param fd Native file descriptor.
     * @return File system block size or negative value if not supported.
     */
    public static int getFileSystemBlockSize(int fd) {
        return fs == null ? -1 : fs.getFileSystemBlockSize(fd);
    }

    /**
     * !!! Use with caution. May produce unexpected results.
     *
     * Known to work correctly on Linux EXT4 and Btrfs,
     * while on XSF it returns meaningful result only after
     * file reopening.
     *
     * @param fd Native file descriptor.
     * @return Approximate system dependent size of the sparse file or negative
     *          value if not supported.
     */
    public static long getSparseFileSize(int fd) {
        return fs == null ? -1 : fs.getSparseFileSize(fd);
    }

    /**
     * @param fd Native file descriptor.
     * @param off Offset of the hole.
     * @param len Length of the hole.
     * @param fsBlockSize File system block size.
     * @return Actual punched hole size.
     */
    public static long punchHole(int fd, long off, long len, int fsBlockSize) {
        assert off >= 0;
        assert len > 0;

        checkSupported();

        if (len < fsBlockSize)
            return 0;

        // TODO maybe optimize for power of 2
        if (off % fsBlockSize != 0) {
            long end = off + len;
            off = (off / fsBlockSize + 1) * fsBlockSize;
            len = end - off;

            if (len <= 0)
                return 0;
        }

        len = len / fsBlockSize * fsBlockSize;

        if (len > 0)
            fs.punchHole(fd, off, len);

        return len;
    }
}
