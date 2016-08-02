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

package org.apache.ignite.igfs;

import org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.LocalIgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.processors.igfs.IgfsDualAbstractSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.igfs.IgfsSecondaryFileSystemTestAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;

/**
 * Abstract test for Hadoop 1.0 file system stack.
 */
public abstract class LocalDualAbstractTest extends IgfsDualAbstractSelfTest {
    /** */
    private static final String FS_WORK_DIR = U.getIgniteHome() + File.separatorChar + "work"
        + File.separatorChar + "fs";

    /** Constructor. */
    public LocalDualAbstractTest(IgfsMode mode) {
        super(mode);
    }

    /**
     * Creates secondary filesystems.
     * @return IgfsSecondaryFileSystem
     * @throws Exception On failure.
     */
    @Override protected IgfsSecondaryFileSystem createSecondaryFileSystemStack() throws Exception {
       final File workDir = new File(FS_WORK_DIR);

        if (!workDir.exists())
            assert workDir.mkdirs();

        CachingHadoopFileSystemFactory factory = new CachingHadoopFileSystemFactory();

        factory.setUri("file:///");

        LocalIgfsSecondaryFileSystem second = new LocalIgfsSecondaryFileSystem();

        second.setWorkDirectory(workDir.getAbsolutePath());

        second.setFileSystemFactory(factory);

        igfsSecondary = new LocalIgfsSecondaryFileSystemTestAdapter(workDir);

        return second;
    }

    /** {@inheritDoc} */
    @Override protected boolean appendSupported() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean permissionsSupported() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean timesSupported() {
        return false;
    }

    /**
     * Adapter for local secondary file system.
     */
    private static class LocalIgfsSecondaryFileSystemTestAdapter implements IgfsSecondaryFileSystemTestAdapter {
        /** */
        private final String workDir;

        /**
         * @param workDir Work dir.
         */
        public LocalIgfsSecondaryFileSystemTestAdapter(final File workDir) {
            this.workDir = workDir.getAbsolutePath();
        }

        /** {@inheritDoc} */
        @Override public String name() throws IOException {
            return "local";
        }

        /** {@inheritDoc} */
        @Override public boolean exists(final String path) throws IOException {
            return Files.exists(path(path));
        }

        /** {@inheritDoc} */
        @Override public boolean delete(final String path, final boolean recursive) throws IOException {
            if (recursive)
                return deleteRecursively(path(path));
            else
                return path(path).toFile().delete();
        }

        /** {@inheritDoc} */
        @Override public void mkdirs(final String path) throws IOException {
            Files.createDirectory(path(path));
        }

        /** {@inheritDoc} */
        @Override public void format() throws IOException {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(workDir))) {
                for (Path innerPath : stream)
                    deleteRecursively(innerPath);
            }
        }

        /** {@inheritDoc} */
        @Override public Map<String, String> properties(final String path) throws IOException {
            // TODO: Create ticket for this!
            throw new UnsupportedOperationException("properties");
        }

        /** {@inheritDoc} */
        @Override public String permissions(String path) throws IOException {
            // TODO: Create ticket for this!
            throw new UnsupportedOperationException("permissions");
        }

        /** {@inheritDoc} */
        @Override public InputStream openInputStream(final String path) throws IOException {
            return Files.newInputStream(path(path));
        }

        /** {@inheritDoc} */
        @Override public OutputStream openOutputStream(final String path, final boolean append) throws IOException {
            if (append)
                return Files.newOutputStream(path(path), StandardOpenOption.APPEND);
            else
                return Files.newOutputStream(path(path));
        }

        /** {@inheritDoc} */
        @Override public T2<Long, Long> times(String path) throws IOException {
            // TODO: Create ticket for this!
            throw new UnsupportedOperationException("times");
        }

        /** {@inheritDoc} */
        @Override public IgfsEx igfs() {
            return null;
        }

        /**
         * Convert path.
         *
         * @param path String path.
         * @return Java File API path.
         */
        private Path path(String path) {
            return Paths.get(workDir + path);
        }

        /**
         * Delete recursively.
         *
         * @param path Path.
         * @throws IOException If failed.
         */
        private boolean deleteRecursively(Path path) throws IOException {
            if (Files.isDirectory(path)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                    for (Path innerPath : stream) {
                        boolean res = deleteRecursively(innerPath);

                        if (!res)
                            return false;
                    }
                }
            }

            return path.toFile().delete();
        }
    }
}