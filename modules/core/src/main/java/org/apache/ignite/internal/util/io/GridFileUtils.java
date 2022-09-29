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

package org.apache.ignite.internal.util.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * General files manipulation utilities.
 */
public class GridFileUtils {
    /** Copy buffer size. */
    private static final int COPY_BUFFER_SIZE = 1024 * 1024;

    /**
     * Copy file
     *
     * @param src Source.
     * @param dst Dst.
     * @param maxBytes Max bytes.
     */
    public static void copy(FileIO src, FileIO dst, long maxBytes) throws IOException {
        assert maxBytes >= 0;

        long bytes = Math.min(src.size(), maxBytes);

        byte[] buf = new byte[COPY_BUFFER_SIZE];

        while (bytes > 0)
            bytes -= dst.writeFully(buf, 0, src.readFully(buf, 0, (int)Math.min(COPY_BUFFER_SIZE, bytes)));

        dst.force();
    }

    /**
     * Copy file
     *
     * @param srcFactory Source factory.
     * @param src Source.
     * @param dstFactory Dst factory.
     * @param dst Dst.
     * @param maxBytes Max bytes.
     */
    public static void copy(
            FileIOFactory srcFactory,
            File src,
            FileIOFactory dstFactory,
            File dst,
            long maxBytes
    ) throws IOException {
        boolean err = true;

        try (FileIO dstIO = dstFactory.create(dst, CREATE, TRUNCATE_EXISTING, WRITE)) {
            try (FileIO srcIO = srcFactory.create(src, READ)) {
                copy(srcIO, dstIO, maxBytes);

                err = false;
            }
        }
        finally {
            if (err)
                dst.delete();
        }
    }

    /**
     * Checks that creating hard links between given paths is available.
     *
     * @param path1 The first path.
     * @param path2 The second path.
     * @throws IgniteCheckedException If creating hard links is not available.
     */
    public static void ensureHardLinkAvailable(Path path1, Path path2) throws IgniteCheckedException {
        try {
            FileStore fs1 = Files.getFileStore(path1);
            FileStore fs2 = Files.getFileStore(path2);

            if (!F.eq(fs1.name(), fs2.name())) {
                throw new IgniteCheckedException("Paths are not stored at the same device or partition. " +
                    "Creating hard links is not available. [path1=" + path1 + ", path2=" + path2 +
                    ", fileStoreName1=" + fs1.name() + ", fileStoreName2=" + fs2.name() + ']');
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to check file stores.", e);
        }
    }
}
