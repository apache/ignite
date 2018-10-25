package org.apache.ignite.internal.processors.compress;

import java.nio.file.Path;

/**
 * Native file system API.
 */
public interface NativeFileSystem {
    /**
     * @param path Path.
     * @return File system block size.
     */
    int getFileBlockSize(Path path);

    /**
     * @param fd Native file descriptor.
     * @param off Offset of the hole.
     * @param len Length of the hole.
     */
    void punchHole(int fd, long off, long len);

    /**
     * @param file Sparse file path.
     * @return Sparse size.
     */
    long getSparseFileSize(Path file);
}
