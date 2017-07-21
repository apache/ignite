/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testframework;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

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