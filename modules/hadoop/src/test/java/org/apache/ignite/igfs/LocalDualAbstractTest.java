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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.IgniteException;
import org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.HadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.LocalIgfsSecondaryFileSystem;
import org.apache.ignite.hadoop.util.ChainedUserNameMapper;
import org.apache.ignite.hadoop.util.KerberosUserNameMapper;
import org.apache.ignite.hadoop.util.UserNameMapper;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.processors.hadoop.fs.HadoopFileSystemsUtils;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsUtils;
import org.apache.ignite.internal.processors.igfs.IgfsDualAbstractSelfTest;
import org.apache.ignite.internal.util.io.GridFilenameUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import static org.apache.ignite.igfs.HadoopSecondaryFileSystemConfigurationTest.SECONDARY_CFG_PATH;
import static org.apache.ignite.igfs.HadoopSecondaryFileSystemConfigurationTest.configuration;
import static org.apache.ignite.igfs.HadoopSecondaryFileSystemConfigurationTest.mkUri;
import static org.apache.ignite.igfs.HadoopSecondaryFileSystemConfigurationTest.writeConfiguration;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * Abstract test for Hadoop 1.0 file system stack.
 */
public abstract class LocalDualAbstractTest extends IgfsDualAbstractSelfTest {
    /** Secondary grid name */
    private static final String GRID_NAME = "grid_secondary";

    /** Secondary file system name */
    private static final String IGFS_NAME = "igfs_secondary";

    /** Secondary file system REST endpoint port */
    private static final int PORT = 11500;

    /** Secondary file system REST endpoint configuration map. */
    private static final IgfsIpcEndpointConfiguration SECONDARY_REST_CFG = new IgfsIpcEndpointConfiguration() {{
        setType(IgfsIpcEndpointType.TCP);
        setPort(PORT);
    }};

    /** Secondary file system authority. */
    private static final String SECONDARY_AUTHORITY = IGFS_NAME + ":" + GRID_NAME + "@127.0.0.1:" + PORT;

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
        startUnderlying();

        KerberosUserNameMapper mapper1 = new KerberosUserNameMapper();

        mapper1.setRealm("TEST.COM");

        TestUserNameMapper mapper2 = new TestUserNameMapper();

        ChainedUserNameMapper mapper = new ChainedUserNameMapper();

        mapper.setMappers(mapper1, mapper2);

        final File workDir = new File(FS_WORK_DIR);

        if (!workDir.exists())
            assert workDir.mkdirs();

        CachingHadoopFileSystemFactory factory = new CachingHadoopFileSystemFactory();

        factory.setUri(mkUri("file", SECONDARY_AUTHORITY));
        factory.setUserNameMapper(mapper);

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
     * Starts underlying Ignite process.
     * @throws IOException On failure.
     */
    protected void startUnderlying() throws Exception {
        startGridWithIgfs(GRID_NAME, IGFS_NAME, PRIMARY, null, SECONDARY_REST_CFG, secondaryIpFinder);
    }

    /**
     * Test user name mapper.
     */
    private static class TestUserNameMapper implements UserNameMapper, LifecycleAware {
        /** */
        private static final long serialVersionUID = 0L;

        /** Started flag. */
        private boolean started;

        /** {@inheritDoc} */
        @Nullable @Override public String map(String name) {
            assert started;
            assert name != null && name.contains("@");

            return name.substring(0, name.indexOf("@"));
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteException {
            started = true;
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteException {
            // No-op.
        }
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
            return super.exists(addParent(path));
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