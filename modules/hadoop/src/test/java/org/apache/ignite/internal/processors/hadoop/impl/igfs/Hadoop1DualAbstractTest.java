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

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.ignite.IgniteException;
import org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.hadoop.util.ChainedUserNameMapper;
import org.apache.ignite.hadoop.util.KerberosUserNameMapper;
import org.apache.ignite.hadoop.util.UserNameMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.processors.igfs.IgfsDualAbstractSelfTest;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteFileSystem.IGFS_SCHEME;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * Abstract test for Hadoop 1.0 file system stack.
 */
public abstract class Hadoop1DualAbstractTest extends IgfsDualAbstractSelfTest {
    /** Secondary Ignite instance name */
    private static final String IGNITE_INSTANCE_NAME = "grid_secondary";

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
    private static final String SECONDARY_AUTHORITY = IGFS_NAME + "@127.0.0.1:" + PORT;

    /** Secondary file system authority. */
    private static final String SECONDARY_WORKDIR = "workdir/test";

    /** Secondary Fs configuration full path. */
    protected String secondaryConfFullPath;

    /** Secondary Fs URI. */
    protected String secondaryUri;

    /** Constructor.
     *
     * @param mode IGFS mode.
     */
    public Hadoop1DualAbstractTest(IgfsMode mode) {
        super(mode);
    }

    /**
     * Creates secondary filesystems.
     * @return IgfsSecondaryFileSystem
     * @throws Exception On failure.
     */
    @Override protected IgfsSecondaryFileSystem createSecondaryFileSystemStack() throws Exception {
        startUnderlying();

        prepareConfiguration();

        KerberosUserNameMapper mapper1 = new KerberosUserNameMapper();

        mapper1.setRealm("TEST.COM");

        TestUserNameMapper mapper2 = new TestUserNameMapper();

        ChainedUserNameMapper mapper = new ChainedUserNameMapper();

        mapper.setMappers(mapper1, mapper2);

        CachingHadoopFileSystemFactory factory = new CachingHadoopFileSystemFactory();

        factory.setUri(secondaryUri);
        factory.setConfigPaths(secondaryConfFullPath);
        factory.setUserNameMapper(mapper);

        IgniteHadoopIgfsSecondaryFileSystem second = new IgniteHadoopIgfsSecondaryFileSystem();

        second.setFileSystemFactory(factory);

        igfsSecondary = new HadoopIgfsSecondaryFileSystemTestAdapter(factory);

        return second;
    }

    /**
     * Starts underlying Ignite process.
     * @throws IOException On failure.
     */
    protected void startUnderlying() throws Exception {
        startGridWithIgfs(IGNITE_INSTANCE_NAME, IGFS_NAME, PRIMARY, null, SECONDARY_REST_CFG, secondaryIpFinder);
    }

    /**
     * Prepares Fs configuration.
     * @throws IOException On failure.
     */
    protected void prepareConfiguration() throws IOException {
        Configuration secondaryConf = HadoopSecondaryFileSystemConfigurationTest.configuration(IGFS_SCHEME,
            SECONDARY_AUTHORITY, true, true);

        secondaryConf.setInt("fs.igfs.block.size", 1024);

        secondaryConfFullPath = HadoopSecondaryFileSystemConfigurationTest.writeConfiguration(secondaryConf,
            HadoopSecondaryFileSystemConfigurationTest.SECONDARY_CFG_PATH);

        secondaryUri = HadoopSecondaryFileSystemConfigurationTest.mkUri(IGFS_SCHEME, SECONDARY_AUTHORITY,
            SECONDARY_WORKDIR);
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
}
