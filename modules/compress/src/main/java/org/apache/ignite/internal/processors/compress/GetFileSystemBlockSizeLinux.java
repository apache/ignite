package org.apache.ignite.internal.processors.compress;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class GetFileSystemBlockSizeLinux implements Function<Path, Integer> {
    /** */
    private final ConcurrentHashMap<Path, Integer> cache = new ConcurrentHashMap<>();

    /** */
    @Override public Integer apply(Path file) {
        if (!LinuxFileSystemLibrary.SUPPORTED)
            return -1;

        Path root;

        try {
            root = file.toRealPath().getRoot();
        }
        catch (IOException e) {
            return -1;
        }

        Integer fsBlockSize = cache.get(root);

        if (fsBlockSize == null)
            cache.put(root, fsBlockSize = LinuxFileSystemLibrary.getFileSystemBlockSize(root));

        return fsBlockSize;
    }
}
