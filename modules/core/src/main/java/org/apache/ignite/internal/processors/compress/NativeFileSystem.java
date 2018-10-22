package org.apache.ignite.internal.processors.compress;

import java.nio.file.Path;

public interface NativeFileSystem {

    int getFileBlockSize(Path path);

    int punchHole(int fd, long off, int len);
}
