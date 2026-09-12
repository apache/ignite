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

package org.apache.ignite.internal.processors.igfs.secondary.local;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * File visitor to get occupied file system size.
 */
public class LocalFileSystemSizeVisitor implements FileVisitor<Path> {
    /** File size accumulator. */
    private long size;

    /** {@inheritDoc} */
    @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        return FileVisitResult.CONTINUE;
    }

    /** {@inheritDoc} */
    @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        size += attrs.size();
        return FileVisitResult.CONTINUE;
    }

    /** {@inheritDoc} */
    @Override public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
    }

    /** {@inheritDoc} */
    @Override public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
    }

    /**
     * @return Total size of visited files.
     */
    public long size() {
        return size;
    }
}
