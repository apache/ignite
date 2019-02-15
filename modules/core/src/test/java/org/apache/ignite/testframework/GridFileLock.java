/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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