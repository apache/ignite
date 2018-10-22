package org.apache.ignite.internal.processors.compress;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

public class NativeFileSystemLinux implements NativeFileSystem {
    /** */
    private final ConcurrentHashMap<Path, Integer> blockSizeCache = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public int getFileBlockSize(Path path) {
        assert LinuxFileSystemLibrary.SUPPORTED;

        Path root;

        try {
            root = path.toRealPath().getRoot();
        }
        catch (IOException e) {
            return -1;
        }

        Integer fsBlockSize = blockSizeCache.get(root);

        if (fsBlockSize == null)
            blockSizeCache.put(root, fsBlockSize = LinuxFileSystemLibrary.getFileSystemBlockSize(root));

        return fsBlockSize;
    }

    /** {@inheritDoc} */
    @Override public int punchHole(int fd, long off, int len) {
        return 0;
    }
}
