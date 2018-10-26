package org.apache.ignite.internal.processors.compress;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import jnr.posix.FileStat;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import org.apache.ignite.IgniteException;

/**
 * Posix file system API.
 */
public class NativeFileSystemPosix implements NativeFileSystem {
    /** */
    private static POSIX posix = POSIXFactory.getPOSIX();

    /** */
    private final ConcurrentHashMap<Path, Integer> fsBlockSizeCache = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public int getFileBlockSize(Path path) {
        Path root;

        try {
            root = path.toRealPath().getRoot();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        Integer fsBlockSize = fsBlockSizeCache.get(root);

        if (fsBlockSize == null) {
            FileStat stat = posix.stat(root.toString());
            fsBlockSize = Math.toIntExact(stat.blockSize());
            fsBlockSizeCache.putIfAbsent(root, fsBlockSize);
        }

        return fsBlockSize;
    }

    /** {@inheritDoc} */
    @Override public void punchHole(int fd, long off, long len) {
        throw new UnsupportedOperationException();
    }
}
