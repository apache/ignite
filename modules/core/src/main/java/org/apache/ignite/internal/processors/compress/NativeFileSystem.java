package org.apache.ignite.internal.processors.compress;

import java.nio.file.Path;

public interface NativeFileSystem {

    int getFileBlockSize(Path path);

    void punchHole(int fd, long off, long len);
}
