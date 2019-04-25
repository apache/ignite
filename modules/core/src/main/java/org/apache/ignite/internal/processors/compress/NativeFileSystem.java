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

/**
 * Native file system API.
 */
public interface NativeFileSystem {
    /**
     * @param path Path.
     * @return File system block size in bytes.
     */
    int getFileSystemBlockSize(Path path);

    /**
     * @param fd Native file descriptor.
     * @return File system block size in bytes.
     */
    int getFileSystemBlockSize(int fd);

    /**
     * @param fd Native file descriptor.
     * @param off Offset of the hole.
     * @param len Length of the hole.
     */
    void punchHole(int fd, long off, long len);

    /**
     * @param fd Native file descriptor.
     * @return Approximate system dependent size of the sparse file.
     */
    long getSparseFileSize(int fd);
}
