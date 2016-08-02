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
import org.apache.ignite.hadoop.fs.HadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.LocalIgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsUtils;
import org.apache.ignite.internal.processors.igfs.IgfsDualAbstractSelfTest;
import org.apache.ignite.internal.util.io.GridFilenameUtils;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
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

        igfsSecondary = new LocalFileSystemAdapter(factory, workDir);

        return second;
    }

    /** {@inheritDoc} */
    @Override protected boolean appendSupported() {
        return false;
    }

    /**
     *
     */
    private static class LocalFileSystemAdapter extends HadoopFileSystemUniversalFileSystemAdapter {
        /** */
        private final File workDir;

        /**
         * @param factory FS factory.
         * @param workDir Work dir.
         */
        public LocalFileSystemAdapter(final HadoopFileSystemFactory factory, final File workDir) {
            super(factory);
            this.workDir = workDir;
        }

        /** {@inheritDoc} */
        @Override public String name() throws IOException {
            return super.name();
        }

        /** {@inheritDoc} */
        @Override public boolean exists(final String path) throws IOException {
            return Files.exists(Paths.get(workDir + path));
        }

        /** {@inheritDoc} */
        @Override public boolean delete(final String path, final boolean recursive) throws IOException {
            return super.delete(addParent(path), recursive);
        }

        /** {@inheritDoc} */
        @Override public void mkdirs(final String path) throws IOException {
            super.mkdirs(addParent(path));
        }

        /** {@inheritDoc} */
        @Override public void format() throws IOException {
            HadoopIgfsUtils.clear(get(), workDir.getAbsolutePath());
        }

        /** {@inheritDoc} */
        @Override public Map<String, String> properties(final String path) throws IOException {
            return super.properties(addParent(path));
        }

        /** {@inheritDoc} */
        @Override public InputStream openInputStream(final String path) throws IOException {
            return super.openInputStream(addParent(path));
        }

        /** {@inheritDoc} */
        @Override public OutputStream openOutputStream(final String path, final boolean append) throws IOException {
            return super.openOutputStream(addParent(path), append);
        }

        /** {@inheritDoc} */
        @Override public <T> T unwrap(final Class<T> cls) {
            return super.unwrap(cls);
        }

        /**
         * @param path Current path.
         * @return Path to add.
         */
        private String addParent(String path) {
            if (path.startsWith("/"))
                path = path.substring(1, path.length());

            return GridFilenameUtils.concat(workDir.getAbsolutePath(), path);
        }
    }
}