package org.apache.ignite.internal.processors.compress;

import java.nio.file.Path;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.util.typedef.internal.U;

public final class FileSystemUtils {
    /** */
    private static final String NATIVE_FS_LINUX_CLASS =
        "org.apache.ignite.internal.processors.compress.NativeFileSystemLinux";

    /** */
    private static final NativeFileSystem fs;

    /** */
    static {
        try {
            NativeFileSystem x = null;

            if (IgniteComponentType.COMPRESSION.inClassPath()) {
                if (U.isLinux())
                    x = U.newInstance(NATIVE_FS_LINUX_CLASS);
            }

            fs = x;
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException(e);
        }
    }

    public static int getFileSystemBlockSize(Path file) {
        assert file != null;

        return fs == null ? -1 : fs.getFileBlockSize(file);
    }

    public static long punchHole(int fd, long off, long len, int fsBlockSize) {
        assert off >= 0;
        assert len > 0;

        if (fs == null || fsBlockSize <= 0)
            return -1;

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
