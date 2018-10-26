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
    @Override public int getFileSystemBlockSize(Path path) {
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
