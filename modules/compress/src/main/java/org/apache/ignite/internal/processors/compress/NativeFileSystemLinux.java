package org.apache.ignite.internal.processors.compress;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

public final class NativeFileSystemLinux implements NativeFileSystem {
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
    public static final int FALLOC_FL_UNSHARE_RANGE	= 0x40;

    /**
     * If the native calls are supported.
     */
    public static final boolean SUPPORTED;

    static {
        boolean ok = false;

        if (Platform.isLinux()) {
            try {
                Native.register(Platform.C_LIBRARY_NAME);
                ok = true;
            }
            catch (RuntimeException e) {
                // No-op.
            }
        }

        SUPPORTED = ok;
    }

    /** */
    private final ConcurrentHashMap<Path, Integer> fsBlockSizeCache = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public boolean isSupported() {
        return SUPPORTED;
    }

    /** {@inheritDoc} */
    @Override public int getFileBlockSize(Path path) {
        if (!SUPPORTED)
            throw new UnsupportedOperationException();

        Path root;

        try {
            root = path.toRealPath().getRoot();
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }

        Integer fsBlockSize = fsBlockSizeCache.get(root);

        if (fsBlockSize == null)
            fsBlockSizeCache.put(root, fsBlockSize = getFileSystemBlockSize(root));

        return fsBlockSize;
    }

    /** {@inheritDoc} */
    @Override public void punchHole(int fd, long off, long len) {
        if (!SUPPORTED)
            throw new UnsupportedOperationException();

        int res = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, off, len);

        if (res != 0)
            throw new IllegalStateException("errno: " + Native.getLastError());
    }

    /** {@inheritDoc} */
    @Override public long getSparseFileSize(Path file) {
        return stat(file.toString()).st_blocks * 512;
    }

    /**
     * Allows the caller to directly manipulate the allocated
     * disk space for the file referred to by fd for the byte range starting
     * at {@code off} offset and continuing for {@code len} bytes.
     *
     * @param fd file descriptor.
     * @param mode determines the operation to be performed on the given range.
     * @param off required position offset.
     * @param len required length.
     * @return On success, fallocate() returns zero.  On error, -1 is returned and
     *        {@code errno} is set to indicate the error.
     * @throws LastErrorException If failed.
     */
    public static native int fallocate(int fd, int mode, long off, long len) throws LastErrorException;

    /**
     * @param path Path.
     * @return File system block size in bytes.
     */
    public static int getFileSystemBlockSize(Path path) {
        return stat(path.toString()).st_blksize;
    }

    /**
     * @param path Path.
     * @return File system info.
     */
    public static native Stat stat(String path);

    static final class Stat {
        /** Total size, in bytes. */
        long st_size;
        /** Block size for filesystem I/O. */
        int st_blksize;
        /** Number of 512B blocks allocated. */
        long st_blocks;
    }
}
