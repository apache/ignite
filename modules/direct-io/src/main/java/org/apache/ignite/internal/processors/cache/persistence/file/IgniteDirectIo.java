package org.apache.ignite.internal.processors.cache.persistence.file;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

public class IgniteDirectIo {

    static {
        Native.register(Platform.C_LIBRARY_NAME);
    }

    public static native int open(String pathname, int flags, int mode);


    /**
     * See "man 2 close"
     *
     * @param fd The file descriptor of the file to close
     *
     * @return 0 on success, -1 on error
     */
    public static native int close(int fd); // musn't forget to do this

    /**
     * @param fd
     * @param buf
     * @param count
     * @param offset
     * @return
     */
    public static native NativeLong pwrite(int fd, Pointer buf, NativeLong count, NativeLong offset);


    public static native NativeLong pread(int fd, Pointer buf, NativeLong count, NativeLong offset);

    /*
    public int pwrite(int fd, AlignedDirectByteBuffer buf, long offset) throws IOException {

        // must always write to end of current block
        // To handle writes past the logical file size,
        // we will later truncate.
        final int start = buf.position();
        assert start == blockStart(start);
        final int toWrite = blockEnd(buf.limit()) - start;

        int n = pwrite(fd, buf.pointer().share(start), new NativeLong(toWrite), new NativeLong(offset)).intValue();
        if (n < 0) {
            throw new IOException("error writing file at offset " + offset + ": " + DirectIoLib.getLastError());
        }
        return n;
    }*/
}
