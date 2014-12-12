/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework;

import org.apache.ignite.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.channels.*;

/**
 * OS-level file lock implementation.
 */
public class GridFileLock {
    /** FS file for lock. */
    private final File file;

    /** Random access file for FS file. */
    private final RandomAccessFile raFile;

    /** File lock. */
    private FileLock fileLock;

    /**
     * Initializes the lock.
     *
     * The constructor opens the lock file, which then should be
     * closed with {@link #close()} method.
     *
     * @param file FS file to use as a lock file.
     * @throws FileNotFoundException If error occurs on opening or creating the file.
     */
    GridFileLock(File file) throws FileNotFoundException {
        this.file = file;

        raFile = new RandomAccessFile(file, "rw");
    }

    /**
     * Performs an exclusive lock on a file, that
     * this lock instance was constructed with.
     *
     * @throws IgniteCheckedException If failed to perform locking. The file remains open.
     */
    public void lock() throws IgniteCheckedException {
        lock(false);
    }

    /**
     * Performs a lock (shared or exclusive) on a file, that
     * this lock instance was constructed with.
     *
     * @param shared Whether a lock is shared (non-exclusive).
     * @throws IgniteCheckedException If failed to perform locking. The file remains open.
     */
    public void lock(boolean shared) throws IgniteCheckedException {
        if (fileLock != null)
            throw new IgniteCheckedException("Already locked [lockFile=" + file + ']');

        try {
            fileLock = raFile.getChannel().tryLock(0, Long.MAX_VALUE, shared);

            if (fileLock == null)
                throw new IgniteCheckedException("Failed to get exclusive lock on lock file [lockFile=" + file + ']');
        }
        catch (IOException | OverlappingFileLockException e) {
            throw new IgniteCheckedException("Failed to get exclusive lock on lock file [lockFile=" + file + ']', e);
        }
    }

    /**
     * Unlocks the file.
     */
    public void unlock() {
        if (fileLock != null) {
            U.releaseQuiet(fileLock);

            fileLock = null;
        }
    }

    /**
     * Unlocks and closes the file.
     */
    public void close() {
        unlock();

        U.closeQuiet(raFile);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridFileLock.class, this);
    }
}
