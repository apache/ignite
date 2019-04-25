/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.compress;

import java.nio.file.Path;
import jnr.posix.FileStat;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;

/**
 * Posix file system API.
 */
public class NativeFileSystemPosix implements NativeFileSystem {
    /** */
    private static POSIX posix = POSIXFactory.getPOSIX();

    /** {@inheritDoc} */
    @Override public int getFileSystemBlockSize(Path path) {
        FileStat stat = posix.stat(path.toString());
        return Math.toIntExact(stat.blockSize());
    }

    /** {@inheritDoc} */
    @Override public int getFileSystemBlockSize(int fd) {
        FileStat stat = posix.fstat(fd);
        return Math.toIntExact(stat.blockSize());
    }

    /** {@inheritDoc} */
    @Override public long getSparseFileSize(int fd) {
        FileStat stat = posix.fstat(fd);
        return stat.blocks() * 512;
    }

    /** {@inheritDoc} */
    @Override public void punchHole(int fd, long off, long len) {
        throw new UnsupportedOperationException();
    }
}
