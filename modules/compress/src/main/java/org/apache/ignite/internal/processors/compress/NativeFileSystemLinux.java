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

import jnr.ffi.LibraryLoader;
import org.apache.ignite.IgniteException;

/**
 * Linux native file system API.
 */
public final class NativeFileSystemLinux extends NativeFileSystemPosix {
    /**
     * default is extend size
     */
    public static final int FALLOC_FL_KEEP_SIZE = 0x01;

    /**
     * de-allocates range
     */
    public static final int FALLOC_FL_PUNCH_HOLE = 0x02;

    /**
     * reserved codepoint
     */
    public static final int FALLOC_FL_NO_HIDE_STALE = 0x04;

    /**
     * FALLOC_FL_COLLAPSE_RANGE is used to remove a range of a file
     * without leaving a hole in the file. The contents of the file beyond
     * the range being removed is appended to the start offset of the range
     * being removed (i.e. the hole that was punched is "collapsed"),
     * resulting in a file layout that looks like the range that was
     * removed never existed. As such collapsing a range of a file changes
     * the size of the file, reducing it by the same length of the range
     * that has been removed by the operation.
     *
     * Different filesystems may implement different limitations on the
     * granularity of the operation. Most will limit operations to
     * filesystem block size boundaries, but this boundary may be larger or
     * smaller depending on the filesystem and/or the configuration of the
     * filesystem or file.
     *
     * Attempting to collapse a range that crosses the end of the file is
     * considered an illegal operation - just use ftruncate(2) if you need
     * to collapse a range that crosses EOF.
     */
    public static final int FALLOC_FL_COLLAPSE_RANGE = 0x08;

    /**
     * FALLOC_FL_ZERO_RANGE is used to convert a range of file to zeros preferably
     * without issuing data IO. Blocks should be preallocated for the regions that
     * span holes in the file, and the entire range is preferable converted to
     * unwritten extents - even though file system may choose to zero out the
     * extent or do whatever which will result in reading zeros from the range
     * while the range remains allocated for the file.
     *
     * This can be also used to preallocate blocks past EOF in the same way as
     * with fallocate. Flag FALLOC_FL_KEEP_SIZE should cause the inode
     * size to remain the same.
     */
    public static final int FALLOC_FL_ZERO_RANGE = 0x10;

    /**
     * FALLOC_FL_INSERT_RANGE is use to insert space within the file size without
     * overwriting any existing data. The contents of the file beyond offset are
     * shifted towards right by len bytes to create a hole.  As such, this
     * operation will increase the size of the file by len bytes.
     *
     * Different filesystems may implement different limitations on the granularity
     * of the operation. Most will limit operations to filesystem block size
     * boundaries, but this boundary may be larger or smaller depending on
     * the filesystem and/or the configuration of the filesystem or file.
     *
     * Attempting to insert space using this flag at OR beyond the end of
     * the file is considered an illegal operation - just use ftruncate(2) or
     * fallocate(2) with mode 0 for such type of operations.
     */
    public static final int FALLOC_FL_INSERT_RANGE = 0x20;

    /**
     * FALLOC_FL_UNSHARE_RANGE is used to unshare shared blocks within the
     * file size without overwriting any existing data. The purpose of this
     * call is to preemptively reallocate any blocks that are subject to
     * copy-on-write.
     *
     * Different filesystems may implement different limitations on the
     * granularity of the operation. Most will limit operations to filesystem
     * block size boundaries, but this boundary may be larger or smaller
     * depending on the filesystem and/or the configuration of the filesystem
     * or file.
     *
     * This flag can only be used with allocate-mode fallocate, which is
     * to say that it cannot be used with the punch, zero, collapse, or
     * insert range modes.
     */
    public static final int FALLOC_FL_UNSHARE_RANGE = 0x40;

    /** */
    private static final LinuxNativeLibC libc = LibraryLoader.create(LinuxNativeLibC.class)
        .failImmediately().load("c");

    /** {@inheritDoc} */
    @Override public void punchHole(int fd, long off, long len) {
        int res = libc.fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, off, len);

        if (res != 0)
            throw new IgniteException("errno: " + res);
    }

    /**
     */
    public interface LinuxNativeLibC {
        /**
         * Allows the caller to directly manipulate the allocated
         * disk space for the file referred to by fd for the byte range starting
         * at {@code off} offset and continuing for {@code len} bytes.
         *
         * @param fd   file descriptor.
         * @param mode determines the operation to be performed on the given range.
         * @param off  required position offset.
         * @param len  required length.
         * @return On success, fallocate() returns zero.  On error, -1 is returned and
         * {@code errno} is set to indicate the error.
         */
        int fallocate(int fd, int mode, long off, long len);
    }
}
